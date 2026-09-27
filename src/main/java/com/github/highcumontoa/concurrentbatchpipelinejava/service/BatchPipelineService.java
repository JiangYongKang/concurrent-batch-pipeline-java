package com.github.highcumontoa.concurrentbatchpipelinejava.service;

import com.github.highcumontoa.concurrentbatchpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchRecord;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchSummary;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordResult;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordStatus;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 并发批处理管线。
 *
 * <p>背压模型：信号量许可数 = concurrency + queueCapacity，调用线程在
 * acquireTimeoutMs 内拿不到许可即对该条记录快速失败（REJECTED_CAPACITY），
 * 不无限堆积、不阻塞调用方。处理线程池固定为 concurrency 大小。
 *
 * <p>幂等模型：先 tryClaim 再处理，已占用键直接返回 DUPLICATE 及可区分原因。
 *
 * <p>失败分类：{@link RetryableProcessingException} 重试至多 maxRetries 次，
 * 耗尽置 DEAD_LETTER；{@link PermanentProcessingException} 及未知异常置
 * FAILED_PERMANENT，不重试。
 *
 * <p>停机收敛：destroy 时停止接收新任务并等待在途任务在 shutdownTimeoutMs
 * 内完成终态落盘；未完成的键在存储中保持 claim 状态，重启后按 DUPLICATE
 * 处理，不产生重复副作用。
 */
@Service
public class BatchPipelineService {

    private static final Logger log = LoggerFactory.getLogger(BatchPipelineService.class);

    private final PipelineProperties properties;
    private final IdempotencyStore store;
    private final RecordProcessor processor;
    private final PipelineMetrics metrics;
    private final Semaphore permits;
    private final ExecutorService executor;
    private volatile boolean shuttingDown = false;

    public BatchPipelineService(PipelineProperties properties, IdempotencyStore store,
                                RecordProcessor processor, PipelineMetrics metrics) {
        this.properties = properties;
        this.store = store;
        this.processor = processor;
        this.metrics = metrics;
        this.permits = new Semaphore(properties.concurrency() + properties.queueCapacity());
        java.util.concurrent.atomic.AtomicInteger threadSeq = new java.util.concurrent.atomic.AtomicInteger();
        this.executor = Executors.newFixedThreadPool(properties.concurrency(),
                r -> new Thread(r, "pipeline-worker-" + threadSeq.incrementAndGet()));
    }

    /**
     * 同步批量提交：为每条记录并发推进至终态后，返回逐条结果与聚合计数。
     */
    public BatchSummary submit(List<BatchRecord> records) {
        List<CompletableFuture<RecordResult>> futures = records.stream()
                .map(this::submitOne)
                .toList();
        List<RecordResult> results = futures.stream().map(CompletableFuture::join).toList();
        Map<RecordStatus, Long> counts = results.stream()
                .collect(Collectors.groupingBy(RecordResult::status, Collectors.counting()));
        log.info("批次完成: total={} counts={} conserved={}", results.size(), counts, metrics.isConserved());
        return new BatchSummary(results.size(), counts, results);
    }

    private CompletableFuture<RecordResult> submitOne(BatchRecord record) {
        metrics.onSubmitted();
        String key = record.businessKey();

        if (shuttingDown) {
            return terminal(RecordStatus.REJECTED_CAPACITY, key, 0, "应用正在关闭，拒绝新提交");
        }
        if (!store.tryClaim(key)) {
            String reason = store.findResult(key)
                    .map(r -> "业务标识已存在终态结果: " + r.status())
                    .orElse("业务标识正在处理中或为停机恢复的在途占用");
            return terminal(RecordStatus.DUPLICATE, key, 0, reason);
        }

        boolean acquired;
        try {
            acquired = permits.tryAcquire(properties.acquireTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return terminal(RecordStatus.REJECTED_CAPACITY, key, 0, "等待处理许可时被中断");
        }
        if (!acquired) {
            // 未承接即释放占用键的语义说明：该记录未产生任何副作用，
            // 以 REJECTED_CAPACITY 作为终态落盘，调用方可凭此原因区分并稍后换键重试。
            RecordResult r = new RecordResult(key, RecordStatus.REJECTED_CAPACITY,
                    "并发许可与等待时限内无法承接(concurrency=" + properties.concurrency()
                            + ", queueCapacity=" + properties.queueCapacity()
                            + ", acquireTimeoutMs=" + properties.acquireTimeoutMs() + ")", 0);
            store.complete(key, r);
            return terminal(r);
        }

        CompletableFuture<RecordResult> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(processWithRetry(record));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                permits.release();
            }
        });
        return future.thenApply(this::persistAndCount);
    }

    private RecordResult processWithRetry(BatchRecord record) {
        String key = record.businessKey();
        int maxAttempts = properties.maxRetries() + 1;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                processor.process(record);
                return new RecordResult(key, RecordStatus.SUCCESS, "处理成功", attempt);
            } catch (RetryableProcessingException e) {
                log.warn("可重试失败: key={} attempt={}/{} reason={}", key, attempt, maxAttempts, e.getMessage());
                if (attempt == maxAttempts) {
                    return new RecordResult(key, RecordStatus.DEAD_LETTER,
                            "可重试失败重试耗尽(" + properties.maxRetries() + "次): " + e.getMessage(), attempt);
                }
            } catch (PermanentProcessingException e) {
                return new RecordResult(key, RecordStatus.FAILED_PERMANENT,
                        "不可恢复失败: " + e.getMessage(), attempt);
            } catch (RuntimeException e) {
                return new RecordResult(key, RecordStatus.FAILED_PERMANENT,
                        "未分类异常按不可恢复处理: " + e.getClass().getSimpleName() + ": " + e.getMessage(), attempt);
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private CompletableFuture<RecordResult> terminal(RecordStatus status, String key, int attempts, String reason) {
        return terminal(new RecordResult(key, status, reason, attempts));
    }

    private CompletableFuture<RecordResult> terminal(RecordResult result) {
        metrics.onTerminal(result.status());
        return CompletableFuture.completedFuture(result);
    }

    /** 处理任务到达终态后落盘并计数。 */
    private RecordResult persistAndCount(RecordResult result) {
        store.complete(result.businessKey(), result);
        metrics.onTerminal(result.status());
        return result;
    }

    @PreDestroy
    public void shutdown() {
        shuttingDown = true;
        executor.shutdown();
        try {
            boolean drained = executor.awaitTermination(properties.shutdownTimeoutMs(), TimeUnit.MILLISECONDS);
            log.info("管线停机收敛: drained={} inFlight={} conserved={}",
                    drained, metrics.inFlight(), metrics.isConserved());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
        }
    }
}
