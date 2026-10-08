package com.github.highcumontoa.pipeline.core;

import com.github.highcumontoa.pipeline.store.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * 背压拒绝 / 关闭竞态发生在形成结论之前，会 releaseClaim 回收占位，
 * 同一标识之后可作为新任务正常重提；已有最终结论的标识不会被回收，重提仍算重复。
 *
 * <p>状态查询：{@link #queryStatus(String)} 只读返回 SUCCEEDED / FAILED /
 * IN_PROGRESS / UNKNOWN 四态，不改变状态、不占容量；最终结论落盘持久化，
 * 重启后查询结果与重启前一致，中断时在途的标识重启后按 UNKNOWN 处理、可重新提交。
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
        if (!store.tryClaim(id)) {
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
            // 被背压挡回时尚未形成任何结论：回收占位，使同一标识稍后可作为新任务重提。
            store.releaseClaim(id);
            return completed(tally(RecordStatus.REJECTED,
                    new RecordResult(id, RecordStatus.REJECTED,
                            "backpressure: no capacity within " + properties.getOfferTimeoutMs() + "ms", 0)));
        }
        CompletableFuture<RecordResult> future = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    future.complete(processWithRetry(record));
                } catch (Throwable t) {
                    RecordResult fatal = new RecordResult(id, RecordStatus.FAILED_FATAL,
                            "unexpected: " + t, properties.getMaxAttempts());
                    // 同样落盘并清掉在途占位，避免标识永久占用、重启后状态含糊。
                    store.saveResult(fatal);
                    future.complete(tally(RecordStatus.FAILED_FATAL, fatal));
                } finally {
                    permits.release();
                }
            });
        } catch (RuntimeException e) {
            // 关闭过程中的竞态：执行器已停，快速拒绝；同样回收占位，允许后续重提。
            permits.release();
            store.releaseClaim(id);
            return completed(tally(RecordStatus.REJECTED,
                    new RecordResult(id, RecordStatus.REJECTED, "engine shutting down", 0)));
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

    /**
     * 按业务标识只读查询当前状态：成功 / 失败 / 处理中 / 未知（未提交或曾被背压挡回）。
     *
     * <p>查询不改变任何状态、不占用处理许可；已有最终结论直接读落盘恢复的结果，
     * 因此重启前后对同一标识查到的结论一致。
     */
    public TaskStatus queryStatus(String businessId) {
        if (businessId == null || businessId.isBlank()) {
            return new TaskStatus(businessId, TaskState.UNKNOWN, null, null, 0);
        }
        Optional<RecordResult> terminal = store.find(businessId);
        if (terminal.isPresent()) {
            RecordResult r = terminal.get();
            TaskState state = r.status() == RecordStatus.PROCESSED ? TaskState.SUCCEEDED : TaskState.FAILED;
            return new TaskStatus(businessId, state, r.status(), r.reason(), r.attempts());
        }
        if (store.isInFlight(businessId)) {
            return new TaskStatus(businessId, TaskState.IN_PROGRESS, null, null, 0);
        }
        return new TaskStatus(businessId, TaskState.UNKNOWN, null, null, 0);
    }

    /** 系统内部为提交保留的标识占用数（在途占位 + 已确认结论），用于观测拒绝回收是否彻底。 */
    public long claimedCount() {
        return store.claimedCount();
    }

    private RecordResult tally(RecordStatus status, RecordResult result) {        counters.get(status).incrementAndGet();
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
