package com.github.highcumontoa.pipeline;

import com.github.highcumontoa.pipeline.core.BatchPipelineEngine;
import com.github.highcumontoa.pipeline.core.BatchRecord;
import com.github.highcumontoa.pipeline.core.PipelineProperties;
import com.github.highcumontoa.pipeline.core.RecordProcessor;
import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import com.github.highcumontoa.pipeline.core.RetryableProcessingException;
import com.github.highcumontoa.pipeline.core.FatalProcessingException;
import com.github.highcumontoa.pipeline.store.FileBackedIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchPipelineEngineTest {

    private static final Logger log = LoggerFactory.getLogger(BatchPipelineEngineTest.class);

    @TempDir
    Path tempDir;

    private PipelineProperties props(int concurrency, int queueCapacity, long offerTimeoutMs, int maxAttempts) {
        PipelineProperties p = new PipelineProperties();
        p.setConcurrency(concurrency);
        p.setQueueCapacity(queueCapacity);
        p.setOfferTimeoutMs(offerTimeoutMs);
        p.setMaxAttempts(maxAttempts);
        p.setShutdownTimeoutMs(10_000);
        p.setStoreFile(tempDir.resolve("store.log").toString());
        return p;
    }

    private FileBackedIdempotencyStore newStore(PipelineProperties p) {
        FileBackedIdempotencyStore store = new FileBackedIdempotencyStore(p.getStoreFile());
        store.load();
        return store;
    }

    private static List<BatchRecord> records(String prefix, int n) {
        return IntStream.range(0, n).mapToObj(i -> new BatchRecord(prefix + "-" + i, "ok")).toList();
    }

    private static void assertConserved(BatchPipelineEngine engine, long expectedSubmitted) {
        Map<RecordStatus, Long> stats = engine.stats();
        long sum = stats.values().stream().mapToLong(Long::longValue).sum();
        log.info("守恒判定依据: submitted={} == sum(stats)={} , stats={}",
                engine.submittedCount(), sum, stats);
        assertEquals(expectedSubmitted, engine.submittedCount());
        assertEquals(engine.submittedCount(), sum, "状态计数必须守恒：每条提交恰好落一个终态");
    }

    @Test
    void concurrentSubmission_allProcessedAndConserved() throws Exception {
        int threads = 8, perThread = 25, total = threads * perThread;
        PipelineProperties p = props(4, 64, 1_000, 3);
        log.info("输入: {} 线程 x {} 条 = {} 条唯一记录; 并发度={}, 队列={}",
                threads, perThread, total, p.getConcurrency(), p.getQueueCapacity());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), r -> { })) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<RecordResult> all = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int offset = t * perThread;
                pool.submit(() -> {
                    try {
                        start.await();
                        List<RecordResult> rs = engine.submitBatch(records("c-" + offset, perThread));
                        synchronized (all) {
                            all.addAll(rs);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            long processed = all.stream().filter(r -> r.status() == RecordStatus.PROCESSED).count();
            log.info("判定依据: 全部 {} 条唯一记录应全部 PROCESSED, 实际 processed={}", total, processed);
            assertEquals(total, all.size());
            assertEquals(total, processed);
            assertConserved(engine, total);
        }
    }

    @Test
    void duplicateSubmission_dedupedAndSurvivesRestart() {
        PipelineProperties p = props(4, 16, 500, 3);
        log.info("输入: 同一 businessId 重复提交; 并发度={}", p.getConcurrency());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), r -> { })) {
            List<RecordResult> first = engine.submitBatch(List.of(new BatchRecord("dup-1", "ok")));
            List<RecordResult> second = engine.submitBatch(List.of(new BatchRecord("dup-1", "ok")));
            log.info("判定依据: 首次={}, 重复={} (原因须可区分)", first.get(0), second.get(0));
            assertEquals(RecordStatus.PROCESSED, first.get(0).status());
            assertEquals(RecordStatus.DUPLICATE, second.get(0).status());
            assertTrue(second.get(0).reason().contains("already confirmed"));
            assertConserved(engine, 2);
        }
        // 重启：新引擎 + 同一存储文件，已确认结果不得丢失、不得重复处理。
        try (BatchPipelineEngine restarted = new BatchPipelineEngine(p, newStore(p), r -> { })) {
            List<RecordResult> afterRestart = restarted.submitBatch(List.of(new BatchRecord("dup-1", "ok")));
            log.info("判定依据: 重启后再提交={}, 应为 DUPLICATE 而非重复处理", afterRestart.get(0));
            assertEquals(RecordStatus.DUPLICATE, afterRestart.get(0).status());
        }
    }

    @Test
    void duplicateSubmission_inFlightDistinguished() throws Exception {
        PipelineProperties p = props(1, 4, 500, 3);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        log.info("输入: 处理期间重复提交同一 businessId; 并发度={}", p.getConcurrency());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), blocking)) {
            var inFlight = java.util.concurrent.CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("inflight-1", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            List<RecordResult> dup = engine.submitBatch(List.of(new BatchRecord("inflight-1", "ok")));
            log.info("判定依据: 在途重复={}，原因应含 in-flight", dup.get(0));
            assertEquals(RecordStatus.DUPLICATE, dup.get(0).status());
            assertTrue(dup.get(0).reason().contains("in-flight"));
            release.countDown();
            assertEquals(RecordStatus.PROCESSED, inFlight.join().get(0).status());
            assertConserved(engine, 2);
        }
    }

    @Test
    void backpressureSaturation_fastRejectAndConserved() throws Exception {
        PipelineProperties p = props(1, 1, 50, 3);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        int total = 10;
        log.info("输入: {} 条记录涌入; 并发度={}, 队列={}, 等待超时={}ms → 容量={}",
                total, p.getConcurrency(), p.getQueueCapacity(), p.getOfferTimeoutMs(),
                p.getConcurrency() + p.getQueueCapacity());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), blocking)) {
            var first = java.util.concurrent.CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("sat-0", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            List<RecordResult> results = engine.submitBatch(records("sat", 9));
            release.countDown();
            first.join();
            long rejected = results.stream().filter(r -> r.status() == RecordStatus.REJECTED).count();
            long admitted = results.stream().filter(r -> r.status() != RecordStatus.REJECTED).count();
            log.info("判定依据: 容量=2(1处理+1排队), 其余应被 REJECTED; rejected={}, admitted={}", rejected, admitted);
            assertTrue(rejected >= 7, "饱和时必须快速拒绝, rejected=" + rejected);
            results.stream().filter(r -> r.status() == RecordStatus.REJECTED)
                    .forEach(r -> assertTrue(r.reason().contains("backpressure")));
            // 等待被接纳的任务完成后再校验守恒
            assertConserved(engine, 10);
        }
    }

    @Test
    void failureClassification_retryableExhaustedVsFatal() {
        PipelineProperties p = props(2, 8, 500, 3);
        RecordProcessor failing = r -> {
            if (r.payload().startsWith("RETRY")) {
                throw new RetryableProcessingException("transient");
            }
            if (r.payload().startsWith("FATAL")) {
                throw new FatalProcessingException("permanent");
            }
        };
        log.info("输入: 1 条可重试失败 + 1 条不可恢复失败; maxAttempts={}", p.getMaxAttempts());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), failing)) {
            List<RecordResult> rs = engine.submitBatch(List.of(
                    new BatchRecord("f-retry", "RETRY"),
                    new BatchRecord("f-fatal", "FATAL")));
            RecordResult retry = rs.get(0), fatal = rs.get(1);
            log.info("判定依据: retryable={} (attempts 应={}), fatal={} (attempts 应=1)",
                    retry, p.getMaxAttempts(), fatal);
            assertEquals(RecordStatus.FAILED_RETRYABLE, retry.status());
            assertEquals(p.getMaxAttempts(), retry.attempts());
            assertEquals(RecordStatus.FAILED_FATAL, fatal.status());
            assertEquals(1, fatal.attempts());
            assertConserved(engine, 2);
        }
    }

    @Test
    void gracefulShutdown_inFlightSettled() throws Exception {
        PipelineProperties p = props(2, 8, 500, 3);
        p.setShutdownTimeoutMs(5_000);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        log.info("输入: 2 条在途任务时关闭引擎; shutdownTimeout={}ms", p.getShutdownTimeoutMs());
        BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), blocking);
        var future = java.util.concurrent.CompletableFuture.supplyAsync(() ->
                engine.submitBatch(records("sd", 2)));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        release.countDown();
        List<RecordResult> rs = future.join();
        engine.close();
        log.info("判定依据: 在途任务应在关闭时收敛为 PROCESSED, 实际={}", rs);
        assertEquals(2, rs.stream().filter(r -> r.status() == RecordStatus.PROCESSED).count());
        // 已确认结果在存储中可查（重启不丢）
        FileBackedIdempotencyStore reloaded = newStore(p);
        assertEquals(2, reloaded.count());
        assertConserved(engine, 2);
    }
}
