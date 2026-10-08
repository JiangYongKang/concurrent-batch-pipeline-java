package com.github.highcumontoa.pipeline.core;

import com.github.highcumontoa.pipeline.store.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 并发批处理引擎。
 *
 * <p>背压模型：许可数 = concurrency + queueCapacity，提交方在 offerTimeoutMs 内
 * 拿不到许可即快速拒绝（REJECTED），不会无限堆积或阻塞。
 *
 * <p>幂等模型：处理前先 tryClaim 占位，同一 businessId 只会有一个执行体进入
 * 处理逻辑；重复提交返回 DUPLICATE 并附可区分原因（已确认 / 在途）。
 *
 * <p>失败分类：{@link RetryableProcessingException} 按 maxAttempts 重试，
 * 耗尽后记 FAILED_RETRYABLE；{@link FatalProcessingException} 不重试，
 * 直接记 FAILED_FATAL；未知异常按不可恢复处理。
 *
 * <p>停机收敛：close() 停止接收新任务并等待在途任务在 shutdownTimeoutMs 内完成，
 * 每个终态结果先落盘再对调用方可见，重启后不丢已确认结果。
 */
public class BatchPipelineEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchPipelineEngine.class);

    private final PipelineProperties properties;
    private final IdempotencyStore store;
    private final RecordProcessor processor;
    private final ExecutorService executor;
    private final Semaphore permits;
    private final Map<RecordStatus, AtomicLong> counters = new EnumMap<>(RecordStatus.class);
    private final AtomicLong submitted = new AtomicLong();
    private volatile boolean closing;

    public BatchPipelineEngine(PipelineProperties properties, IdempotencyStore store, RecordProcessor processor) {
        this.properties = properties;
        this.store = store;
        this.processor = processor;
        this.executor = Executors.newFixedThreadPool(properties.getConcurrency(),
                r -> {
                    Thread t = new Thread(r, "pipeline-worker");
                    t.setDaemon(true);
                    return t;
                });
        this.permits = new Semaphore(properties.getConcurrency() + properties.getQueueCapacity());
        for (RecordStatus s : RecordStatus.values()) {
            counters.put(s, new AtomicLong());
        }
        log.info("pipeline engine started: concurrency={}, queueCapacity={}, offerTimeoutMs={}, maxAttempts={}",
                properties.getConcurrency(), properties.getQueueCapacity(),
                properties.getOfferTimeoutMs(), properties.getMaxAttempts());
    }

    /** 提交一批记录，逐条返回最终状态（每条记录恰好落一个终态）。 */
    public List<RecordResult> submitBatch(List<BatchRecord> records) {
        List<CompletableFuture<RecordResult>> futures = new ArrayList<>(records.size());
        for (BatchRecord record : records) {
            submitted.incrementAndGet();
            futures.add(submitOne(record));
        }
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private CompletableFuture<RecordResult> submitOne(BatchRecord record) {
        if (record == null || record.businessId() == null || record.businessId().isBlank()) {
            return completed(tally(RecordStatus.REJECTED,
                    new RecordResult(record == null ? null : record.businessId(),
                            RecordStatus.REJECTED, "invalid record: blank businessId", 0)));
        }
        String id = record.businessId();
        IdempotencyStore.ClaimOutcome outcome = store.acquireClaim(id);
        if (outcome != IdempotencyStore.ClaimOutcome.ACCEPTED) {
            Optional<RecordResult> existing = store.find(id);
            String reason = existing.map(r -> "duplicate: already confirmed as " + r.status())
                    .orElse("duplicate: submission already in-flight");
            return completed(tally(RecordStatus.DUPLICATE,
                    new RecordResult(id, RecordStatus.DUPLICATE, reason, 0)));
        }
        boolean admitted;
        try {
            admitted = permits.tryAcquire(properties.getOfferTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            admitted = false;
        }
        if (!admitted || closing) {
            // 尚未开始处理：回滚占位。该标识之后可凭同一 ID 重新提交，不被卡死，
            // 内部占用也随之回收（仅在内存中存在过，不留持久化痕迹）。
            store.rollbackClaim(id);
            String reason = closing
                    ? "rejected: engine shutting down"
                    : "backpressure: no capacity within " + properties.getOfferTimeoutMs() + "ms";
            return completed(tally(RecordStatus.REJECTED,
                    new RecordResult(id, RecordStatus.REJECTED, reason, 0)));
        }
        CompletableFuture<RecordResult> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    // 业务副作用发生前先写"处理已开始"日志：处理途中被中断时，
                    // 重启回放会把该标识识别为 INTERRUPTED，可重新受理，系统绝不伪造结论。
                    store.persistAttemptStarted(id);
                    future.complete(processWithRetry(record));
                } catch (UncheckedIOException e) {
                    // 前置日志写不进盘：不能在无崩溃记录的情况下继续处理，置为中断未决。
                    store.markInterrupted(id);
                    future.complete(tally(RecordStatus.REJECTED,
                            new RecordResult(id, RecordStatus.REJECTED,
                                    "rejected: failed to record processing start: " + e.getMessage(), 0)));
                } catch (Throwable t) {
                    RecordResult fatal = new RecordResult(id, RecordStatus.FAILED_FATAL,
                            "unexpected: " + t, properties.getMaxAttempts());
                    try {
                        store.saveResult(fatal);
                    } catch (UncheckedIOException persistFailure) {
                        // 终态也落不了盘：保留为中断未决（已有 C 行），重启后可重提，
                        // 绝不让调用方无限挂起，也不伪造一个未持久化的"已确认"。
                        log.error("failed to persist terminal result for {}; marking interrupted", id, persistFailure);
                        store.markInterrupted(id);
                        future.complete(tally(RecordStatus.REJECTED,
                                new RecordResult(id, RecordStatus.REJECTED,
                                        "rejected: terminal result could not be persisted, resubmission required: "
                                                + persistFailure.getMessage(), 0)));
                        return;
                    }
                    future.complete(tally(RecordStatus.FAILED_FATAL, fatal));
                } finally {
                    permits.release();
                }
            });
        } catch (RuntimeException e) {
            // 关闭过程中的竞态：任务未开始，回滚占位、释放许可、快速拒绝。
            permits.release();
            store.rollbackClaim(id);
            return completed(tally(RecordStatus.REJECTED,
                    new RecordResult(id, RecordStatus.REJECTED, "rejected: engine shutting down", 0)));
        }
        return future;
    }

    private RecordResult processWithRetry(BatchRecord record) {
        String id = record.businessId();
        int maxAttempts = properties.getMaxAttempts();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                processor.process(record);
                RecordResult ok = new RecordResult(id, RecordStatus.PROCESSED, "processed", attempt);
                store.saveResult(ok);
                return tally(RecordStatus.PROCESSED, ok);
            } catch (RetryableProcessingException e) {
                log.warn("retryable failure id={} attempt={}/{}: {}", id, attempt, maxAttempts, e.getMessage());
                if (attempt == maxAttempts) {
                    RecordResult r = new RecordResult(id, RecordStatus.FAILED_RETRYABLE,
                            "retryable failure, attempts exhausted: " + e.getMessage(), attempt);
                    store.saveResult(r);
                    return tally(RecordStatus.FAILED_RETRYABLE, r);
                }
            } catch (FatalProcessingException e) {
                RecordResult r = new RecordResult(id, RecordStatus.FAILED_FATAL,
                        "fatal failure: " + e.getMessage(), attempt);
                store.saveResult(r);
                return tally(RecordStatus.FAILED_FATAL, r);
            } catch (RuntimeException e) {
                RecordResult r = new RecordResult(id, RecordStatus.FAILED_FATAL,
                        "unexpected failure treated as fatal: " + e, attempt);
                store.saveResult(r);
                return tally(RecordStatus.FAILED_FATAL, r);
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private RecordResult tally(RecordStatus status, RecordResult result) {
        counters.get(status).incrementAndGet();
        return result;
    }

    private static CompletableFuture<RecordResult> completed(RecordResult r) {
        return CompletableFuture.completedFuture(r);
    }

    /** 各状态计数快照；守恒校验：sum(stats) == submittedCount()。 */
    public Map<RecordStatus, Long> stats() {
        Map<RecordStatus, Long> snapshot = new EnumMap<>(RecordStatus.class);
        counters.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    public long submittedCount() {
        return submitted.get();
    }

    /** 当前进程内已受理、尚未出结论的标识数（真实占用处理许可的任务数）。 */
    public long inFlightCount() {
        return store.inFlightCount();
    }

    /** 有"已开始处理"记录但无终态（重启回放识别出的中断未决）的标识数。 */
    public long interruptedCount() {
        return store.interruptedCount();
    }

    /**
     * 只读查询某业务标识当前处于什么情况（CONFIRMED / IN_FLIGHT / INTERRUPTED / NOT_FOUND）。
     * 不改变状态、不占用处理许可。
     */
    public RecordSnapshot lookup(String businessId) {
        if (businessId == null || businessId.isBlank()) {
            return new RecordSnapshot(businessId, SubmissionState.NOT_FOUND, null,
                    "no submission known for blank businessId", 0);
        }
        return store.lookup(businessId);
    }

    /** 停机收敛：拒绝新提交，等待在途任务在 shutdownTimeoutMs 内完成并落盘。 */
    @Override
    public void close() {
        closing = true;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(properties.getShutdownTimeoutMs(), TimeUnit.MILLISECONDS)) {
                log.warn("shutdown timeout after {}ms, forcing", properties.getShutdownTimeoutMs());
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        log.info("pipeline engine closed: stats={}, submitted={}", stats(), submittedCount());
    }
}
