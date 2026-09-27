package com.github.highcumontoa.concurrentbatchpipelinejava.tests;

import com.github.highcumontoa.concurrentbatchpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchRecord;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchSummary;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordResult;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordStatus;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.BatchPipelineService;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.FileBackedIdempotencyStore;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.PipelineMetrics;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.RecordProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 管线行为测试：并发提交、重复提交、容量饱和、失败分类。
 * 每个用例打印输入规模、并发度与判定依据。
 */
class BatchPipelineServiceTest {

    private static final Logger log = LoggerFactory.getLogger(BatchPipelineServiceTest.class);

    @TempDir
    Path tempDir;

    private BatchPipelineService pipeline;
    private PipelineMetrics metrics;

    private BatchPipelineService newPipeline(PipelineProperties props, RecordProcessor processor) {
        metrics = new PipelineMetrics();
        pipeline = new BatchPipelineService(props,
                new FileBackedIdempotencyStore(tempDir.resolve("store.log").toString()),
                processor, metrics);
        return pipeline;
    }

    @AfterEach
    void tearDown() {
        if (pipeline != null) {
            pipeline.shutdown();
        }
    }

    private static PipelineProperties props(int concurrency, int queueCapacity,
                                            long acquireTimeoutMs, int maxRetries) {
        return new PipelineProperties(concurrency, queueCapacity, acquireTimeoutMs,
                maxRetries, 5000, "unused");
    }

    @Test
    void concurrentSubmit_allSucceed_andMetricsConserved() throws Exception {
        int threads = 8, batchSize = 10;
        log.info("[并发提交] 输入: {} 线程 x {} 条唯一键, 并发度=4, 判定依据: 全部 SUCCESS 且计数守恒",
                threads, batchSize);
        newPipeline(props(4, 16, 2000, 0), record -> {
        });

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<BatchSummary> summaries = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.execute(() -> {
                await(start);
                List<BatchRecord> batch = new ArrayList<>();
                for (int i = 0; i < batchSize; i++) {
                    batch.add(new BatchRecord("k-" + tid + "-" + i, "ok"));
                }
                synchronized (summaries) {
                    summaries.add(pipeline.submit(batch));
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long success = summaries.stream().flatMap(s -> s.results().stream())
                .filter(r -> r.status() == RecordStatus.SUCCESS).count();
        log.info("[并发提交] 结果: success={}/{} inFlight={} conserved={}",
                success, threads * batchSize, metrics.inFlight(), metrics.isConserved());
        assertEquals(threads * batchSize, success);
        assertEquals(threads * batchSize, metrics.submitted());
        assertEquals(0, metrics.inFlight());
        assertTrue(metrics.isConserved(), "submitted 必须等于终态之和 + inFlight");
    }

    @Test
    void duplicateSubmit_exactlyOneSideEffect_andDistinguishableReasons() throws Exception {
        int threads = 10;
        log.info("[重复提交] 输入: {} 线程同时提交同一业务键 dup-key, 判定依据: 恰好 1 条 SUCCESS, "
                + "其余 DUPLICATE 且处理器仅执行 1 次", threads);
        AtomicInteger invocations = new AtomicInteger();
        newPipeline(props(4, 16, 2000, 0), record -> {
            invocations.incrementAndGet();
            sleep(100); // 拉宽在途窗口，使并发重复更可能命中"处理中"分支
        });

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<RecordResult> results = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < threads; i++) {
            pool.execute(() -> {
                await(start);
                results.addAll(pipeline.submit(List.of(new BatchRecord("dup-key", "ok"))).results());
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long success = results.stream().filter(r -> r.status() == RecordStatus.SUCCESS).count();
        long duplicate = results.stream().filter(r -> r.status() == RecordStatus.DUPLICATE).count();
        results.stream().filter(r -> r.status() == RecordStatus.DUPLICATE)
                .forEach(r -> log.info("[重复提交] 去重原因: {}", r.reason()));
        log.info("[重复提交] 结果: success={} duplicate={} 处理器实际执行={} conserved={}",
                success, duplicate, invocations.get(), metrics.isConserved());
        assertEquals(1, success, "同一业务标识只允许一次成功副作用");
        assertEquals(threads - 1, duplicate);
        assertEquals(1, invocations.get(), "处理器不得被重复执行");
        assertTrue(metrics.isConserved());
    }

    @Test
    void capacitySaturated_fastReject_withExplicitReason() throws Exception {
        int threads = 6;
        log.info("[容量饱和] 输入: {} 线程各提交 1 条, 并发度=1 队列=0 等待=50ms, 处理耗时 300ms, "
                + "判定依据: 1 条 SUCCESS, 其余 REJECTED_CAPACITY", threads);
        newPipeline(props(1, 0, 50, 0), record -> sleep(300));

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<RecordResult> results = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < threads; i++) {
            final int id = i;
            pool.execute(() -> {
                await(start);
                results.addAll(pipeline.submit(List.of(new BatchRecord("sat-" + id, "ok"))).results());
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        long success = results.stream().filter(r -> r.status() == RecordStatus.SUCCESS).count();
        long rejected = results.stream().filter(r -> r.status() == RecordStatus.REJECTED_CAPACITY).count();
        results.stream().filter(r -> r.status() == RecordStatus.REJECTED_CAPACITY)
                .forEach(r -> log.info("[容量饱和] 拒绝原因: {}", r.reason()));
        log.info("[容量饱和] 结果: success={} rejected={} conserved={}", success, rejected, metrics.isConserved());
        assertEquals(1, success);
        assertEquals(threads - 1, rejected);
        assertTrue(metrics.isConserved());
    }

    @Test
    void failureClassification_retryableExhaustedVsPermanent() {
        log.info("[失败分类] 输入: 1 条 RETRYABLE + 1 条 PERMANENT, maxRetries=2, "
                + "判定依据: 前者 DEAD_LETTER 且 attempts=3, 后者 FAILED_PERMANENT 且 attempts=1");
        AtomicInteger retryableAttempts = new AtomicInteger();
        newPipeline(props(2, 4, 1000, 2), record -> {
            String p = record.payload();
            if (p.startsWith("RETRYABLE:")) {
                retryableAttempts.incrementAndGet();
                throw new com.github.highcumontoa.concurrentbatchpipelinejava.service
                        .RetryableProcessingException("临时故障");
            }
            throw new com.github.highcumontoa.concurrentbatchpipelinejava.service
                    .PermanentProcessingException("数据非法");
        });

        BatchSummary summary = pipeline.submit(List.of(
                new BatchRecord("r1", "RETRYABLE:临时故障"),
                new BatchRecord("p1", "PERMANENT:数据非法")));

        RecordResult r1 = summary.results().get(0);
        RecordResult p1 = summary.results().get(1);
        log.info("[失败分类] 结果: r1={} attempts={} reason={} | p1={} attempts={} reason={}",
                r1.status(), r1.attempts(), r1.reason(), p1.status(), p1.attempts(), p1.reason());
        assertEquals(RecordStatus.DEAD_LETTER, r1.status());
        assertEquals(3, r1.attempts(), "maxRetries=2 应共执行 3 次");
        assertEquals(3, retryableAttempts.get());
        assertEquals(RecordStatus.FAILED_PERMANENT, p1.status());
        assertEquals(1, p1.attempts(), "不可恢复失败不得重试");
        assertTrue(metrics.isConserved());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
