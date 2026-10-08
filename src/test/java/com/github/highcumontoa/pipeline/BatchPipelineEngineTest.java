package com.github.highcumontoa.pipeline;

import com.github.highcumontoa.pipeline.core.BatchPipelineEngine;
import com.github.highcumontoa.pipeline.core.BatchRecord;
import com.github.highcumontoa.pipeline.core.PipelineProperties;
import com.github.highcumontoa.pipeline.core.RecordProcessor;
import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import com.github.highcumontoa.pipeline.core.TaskState;
import com.github.highcumontoa.pipeline.core.TaskStatus;
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
    void queryStatus_fourStatesAndReadOnly() throws Exception {
        PipelineProperties p = props(1, 2, 500, 3);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            if ("prog-1".equals(r.businessId())) {
                entered.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (r.payload() != null && r.payload().startsWith("FATAL")) {
                throw new FatalProcessingException("permanent");
            }
        };
        log.info("输入: 4 个标识分别对应 未知/在途/成功/失败; 并发度={}", p.getConcurrency());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), blocking)) {
            // UNKNOWN：从未提交
            assertEquals(TaskState.UNKNOWN, engine.queryStatus("never-1").state());
            assertEquals(0, engine.claimedCount(), "纯查询不得产生占用");

            // IN_PROGRESS：查询不改变状态、不占容量
            var future = java.util.concurrent.CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("prog-1", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            TaskStatus inProgress = engine.queryStatus("prog-1");
            TaskStatus queriedAgain = engine.queryStatus("prog-1");
            log.info("判定依据: 在途期间连续查询 {} / {}（均应为 IN_PROGRESS，且占用恒为 1）",
                    inProgress, queriedAgain);
            assertEquals(TaskState.IN_PROGRESS, inProgress.state());
            assertEquals(TaskState.IN_PROGRESS, queriedAgain.state());
            assertEquals(1, engine.claimedCount());
            release.countDown();
            future.join();

            // SUCCEEDED / FAILED
            engine.submitBatch(List.of(
                    new BatchRecord("ok-1", "ok"),
                    new BatchRecord("fatal-1", "FATAL")));
            assertEquals(TaskState.SUCCEEDED, engine.queryStatus("ok-1").state());
            assertEquals(TaskState.FAILED, engine.queryStatus("fatal-1").state());
            assertEquals(RecordStatus.FAILED_FATAL, engine.queryStatus("fatal-1").recordStatus());
            // 被背压挡回的标识查询也是 UNKNOWN（本配置队列小，借助一次饱和制造拒绝）
            assertEquals(TaskState.UNKNOWN, engine.queryStatus("never-1").state());
            log.info("判定依据: 四态均可区分：UNKNOWN / IN_PROGRESS / SUCCEEDED / FAILED，查询不改变状态");
        }
    }

    @Test
    void backpressureRejected_sameIdCanResubmitAndSettles() throws Exception {
        // 容量=1(1 处理 + 0 排队)，anchor 占住唯一许可，其余提交确定性地被背压挡回。
        PipelineProperties p = props(1, 0, 50, 3);
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
        String id = "rb-1";
        log.info("输入: 同一标识 {} 在容量={} 下连续重提 3 次被挡回; 并发度={}, 队列={}, 超时={}ms",
                id, p.getConcurrency() + p.getQueueCapacity(),
                p.getConcurrency(), p.getQueueCapacity(), p.getOfferTimeoutMs());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), blocking)) {
            var anchor = java.util.concurrent.CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("anchor", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            for (int i = 1; i <= 3; i++) {
                List<RecordResult> rejected = engine.submitBatch(List.of(new BatchRecord(id, "ok")));
                assertEquals(RecordStatus.REJECTED, rejected.get(0).status(), "第 " + i + " 次应被背压挡回");
                assertTrue(rejected.get(0).reason().contains("backpressure"));
            }
            TaskStatus whileRejected = engine.queryStatus(id);
            long claimedWhileSaturated = engine.claimedCount();
            log.info("判定依据: 被挡回期间查询={}（应为 UNKNOWN）, 系统占用={}（只应含在途 anchor）",
                    whileRejected, claimedWhileSaturated);
            assertEquals(TaskState.UNKNOWN, whileRejected.state());
            assertEquals(1, claimedWhileSaturated, "被挡回的标识不得留下占位");

            // 高峰过去：同一标识作为新任务正常受理并给出真实结论。
            release.countDown();
            anchor.join();
            List<RecordResult> resubmit = engine.submitBatch(List.of(new BatchRecord(id, "ok")));
            log.info("判定依据: 释放容量后重提={}（应为 PROCESSED）", resubmit.get(0));
            assertEquals(RecordStatus.PROCESSED, resubmit.get(0).status());

            TaskStatus settled = engine.queryStatus(id);
            log.info("判定依据: 处理完成后查询={}（应为 SUCCEEDED/PROCESSED）", settled);
            assertEquals(TaskState.SUCCEEDED, settled.state());
            assertEquals(RecordStatus.PROCESSED, settled.recordStatus());

            // 只有已有最终结论之后的重提才算重复。
            List<RecordResult> again = engine.submitBatch(List.of(new BatchRecord(id, "ok")));
            log.info("判定依据: 已有最终结论后再提={}（应为 DUPLICATE/already confirmed）", again.get(0));
            assertEquals(RecordStatus.DUPLICATE, again.get(0).status());
            assertTrue(again.get(0).reason().contains("already confirmed"));
            assertConserved(engine, 1 + 3 + 1 + 1);
        }
    }

    @Test
    void repeatedRejection_internalClaimsDoNotGrow() throws Exception {
        // 容量=1：anchor 占住处理位；对同一批 N 个标识做多轮"拒绝→放行→受理"循环。
        PipelineProperties p = props(1, 0, 20, 3);
        int ids = 8, rejectRounds = 5;
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            if ("anchor".equals(r.businessId())) {
                entered.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        log.info("输入: {} 个标识 x {} 轮拒绝（共 {} 次被挡回）; 容量={}, 超时={}ms",
                ids, rejectRounds, ids * rejectRounds,
                p.getConcurrency() + p.getQueueCapacity(), p.getOfferTimeoutMs());
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), blocking)) {
            var anchor = java.util.concurrent.CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("anchor", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            for (int round = 1; round <= rejectRounds; round++) {
                List<RecordResult> rs = engine.submitBatch(records("grow", ids));
                long rejected = rs.stream().filter(r -> r.status() == RecordStatus.REJECTED).count();
                // 高峰期间：除 anchor 外无任何残留占位，占用不随拒绝次数膨胀。
                assertEquals(ids, rejected, "第 " + round + " 轮应全部被挡回");
                assertEquals(1, engine.claimedCount(),
                        "第 " + round + " 轮拒绝后占用必须回落到只剩在途 anchor，实际=" + engine.claimedCount());
            }
            log.info("判定依据: {} 次拒绝后系统占用仍={}（只含在途 anchor）",
                    ids * rejectRounds, engine.claimedCount());

            // 高峰过去：所有被挡过的标识都能作为新任务受理，无一卡死。
            release.countDown();
            anchor.join();
            List<RecordResult> settled = engine.submitBatch(records("grow", ids));
            long processed = settled.stream().filter(r -> r.status() == RecordStatus.PROCESSED).count();
            log.info("判定依据: 放行后 {} 个标识全部 PROCESSED, 实际 processed={}, 最终占用={}",
                    ids, processed, engine.claimedCount());
            assertEquals(ids, processed);
            assertEquals(ids + 1L, engine.claimedCount(), "占用 = 全部真实结论（anchor + 本批）");

            // 落盘日志中每个标识只能有一条最终结论，绝不能"同一标识落两份结论"。
            List<String> lines = java.nio.file.Files.readAllLines(Path.of(p.getStoreFile()));
            long distinct = lines.stream().filter(l -> !l.isBlank())
                    .map(l -> l.substring(0, l.indexOf('\t'))).distinct().count();
            log.info("判定依据: 落盘 {} 行结论, 去重后标识数={}（应均为 {}），拒绝记录不得落盘",
                    lines.size(), distinct, ids + 1);
            assertEquals(lines.size(), distinct);
            assertEquals(ids + 1L, distinct);
            assertConserved(engine, 1 + (long) ids * rejectRounds + ids);
        }
    }

    @Test
    void restart_confirmedResultsSurviveAndInterruptedInFlightIsResubmittable() throws Exception {
        PipelineProperties p = props(2, 4, 200, 3);
        p.setShutdownTimeoutMs(200);
        log.info("输入: 1 条成功 + 1 条失败先落定，1 条在途任务被强制中断（模拟 kill -9）; 并发度={}",
                p.getConcurrency());

        // ---- 中断前：成功、失败各一条落盘；interrupted-1 处理到一半 ----
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch neverRelease = new CountDownLatch(1);
        RecordProcessor processor = r -> {
            if ("interrupted-1".equals(r.businessId())) {
                entered.countDown();
                // 忽略中断、永久阻塞：模拟进程被外部杀死时线程根本没有机会形成/落盘结论。
                while (true) {
                    try {
                        neverRelease.await();
                        return;
                    } catch (InterruptedException e) {
                        // 被 shutdownNow 打断也继续卡住
                    }
                }
            }
            if (r.payload() != null && r.payload().startsWith("FATAL")) {
                throw new FatalProcessingException("permanent");
            }
        };
        BatchPipelineEngine crashed = new BatchPipelineEngine(p, newStore(p), processor);
        java.util.concurrent.CompletableFuture.supplyAsync(() ->
                crashed.submitBatch(List.of(new BatchRecord("interrupted-1", "ok"))));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        List<RecordResult> before = crashed.submitBatch(List.of(
                new BatchRecord("ok-1", "ok"),
                new BatchRecord("bad-1", "FATAL")));
        assertEquals(RecordStatus.PROCESSED, before.get(0).status());
        assertEquals(RecordStatus.FAILED_FATAL, before.get(1).status());

        // shutdownNow 强制关闭、不等待在途收敛——在途任务没有最终结论（模拟进程被中断）。
        crashed.close();
        List<String> linesAtCrash = java.nio.file.Files.readAllLines(Path.of(p.getStoreFile()));
        boolean interruptedRecorded = linesAtCrash.stream()
                .anyMatch(l -> l.startsWith("interrupted-1\t"));
        log.info("判定依据: 强制中断后落盘 {} 行, interrupted-1 是否有结论={}（应为 false）",
                linesAtCrash.size(), interruptedRecorded);
        assertEquals(2, linesAtCrash.size());
        assertTrue(!interruptedRecorded);

        // ---- 重启：新引擎 + 同一存储文件 ----
        FileBackedIdempotencyStore restartedStore = newStore(p);
        try (BatchPipelineEngine restarted = new BatchPipelineEngine(p, restartedStore, r -> { })) {
            TaskStatus okStatus = restarted.queryStatus("ok-1");
            TaskStatus badStatus = restarted.queryStatus("bad-1");
            TaskStatus interruptedStatus = restarted.queryStatus("interrupted-1");
            log.info("判定依据: 重启后查询 ok-1={}, bad-1={}, interrupted-1={}（依次应为 SUCCEEDED/FAILED/UNKNOWN）",
                    okStatus, badStatus, interruptedStatus);
            assertEquals(TaskState.SUCCEEDED, okStatus.state());
            assertEquals(RecordStatus.PROCESSED, okStatus.recordStatus());
            assertEquals(TaskState.FAILED, badStatus.state());
            assertEquals(RecordStatus.FAILED_FATAL, badStatus.recordStatus());
            assertEquals(TaskState.UNKNOWN, interruptedStatus.state(),
                    "中断时没有结论的标识重启后必须按未知处理，不能谎称处理过");
            assertEquals(2, restarted.claimedCount(), "重启后只应有已确认结论占用，在途占位不恢复");
            assertEquals(2, restartedStore.count());

            // 已确认结论：重提仍算重复，且结论明细不变（一条不丢、不重复处理）。
            List<RecordResult> dupOk = restarted.submitBatch(List.of(new BatchRecord("ok-1", "ok")));
            List<RecordResult> dupBad = restarted.submitBatch(List.of(new BatchRecord("bad-1", "FATAL")));
            log.info("判定依据: 重启后重提已确认标识: ok-1={}, bad-1={}（均应 DUPLICATE）",
                    dupOk.get(0), dupBad.get(0));
            assertEquals(RecordStatus.DUPLICATE, dupOk.get(0).status());
            assertEquals(RecordStatus.DUPLICATE, dupBad.get(0).status());

            // 在途被中断的标识：作为新任务正常受理，给出真实、唯一的结论。
            List<RecordResult> redo = restarted.submitBatch(List.of(new BatchRecord("interrupted-1", "ok")));
            log.info("判定依据: 重启后重提被中断标识={}（应为 PROCESSED，而非 DUPLICATE）", redo.get(0));
            assertEquals(RecordStatus.PROCESSED, redo.get(0).status());
            assertEquals(TaskState.SUCCEEDED, restarted.queryStatus("interrupted-1").state());
        }

        // 落盘日志中每个标识恰好一条结论：没有"同一标识落两份结论"。
        List<String> lines = java.nio.file.Files.readAllLines(Path.of(p.getStoreFile()));
        long distinct = lines.stream().filter(l -> !l.isBlank())
                .map(l -> l.substring(0, l.indexOf('\t'))).distinct().count();
        log.info("判定依据: 最终落盘 {} 行结论, 去重后标识数={}（应相等，共 3 个标识）", lines.size(), distinct);
        assertEquals(lines.size(), distinct);
        assertEquals(3, distinct);
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
