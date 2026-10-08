package com.github.highcumontoa.pipeline;

import com.github.highcumontoa.pipeline.core.BatchPipelineEngine;
import com.github.highcumontoa.pipeline.core.BatchRecord;
import com.github.highcumontoa.pipeline.core.PipelineProperties;
import com.github.highcumontoa.pipeline.core.RecordProcessor;
import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordSnapshot;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import com.github.highcumontoa.pipeline.core.SubmissionState;
import com.github.highcumontoa.pipeline.store.FileBackedIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 新增行为覆盖：
 * 1. 容量拒绝后同一 businessId 仍可正常重提并拿到真实结论；
 * 2. 反复容量拒绝下内部占用（在途/中断/终态记录）不膨胀；
 * 3. 重启后已确认结论查询一致、再次提交仍判重；
 * 4. 处理途中被"杀死"（有开始日志无终态）：重启后查询为 INTERRUPTED，
 *    重新提交被正常受理并落唯一一个结论；
 * 5. 查询只读：不改变状态、不占用处理能力。
 */
class BatchRecoveryAndQueryTest {

    private static final Logger log = LoggerFactory.getLogger(BatchRecoveryAndQueryTest.class);

    @TempDir
    Path tempDir;

    private PipelineProperties props(int concurrency, int queueCapacity, long offerTimeoutMs) {
        PipelineProperties p = new PipelineProperties();
        p.setConcurrency(concurrency);
        p.setQueueCapacity(queueCapacity);
        p.setOfferTimeoutMs(offerTimeoutMs);
        p.setMaxAttempts(3);
        p.setShutdownTimeoutMs(2_000);
        p.setStoreFile(tempDir.resolve("store-" + System.nanoTime() + ".log").toString());
        return p;
    }

    private FileBackedIdempotencyStore newStore(PipelineProperties p) {
        FileBackedIdempotencyStore store = new FileBackedIdempotencyStore(p.getStoreFile());
        store.load();
        return store;
    }

    @Test
    void rejectedByCapacity_sameIdResubmittedAndProcessed() throws Exception {
        PipelineProperties p = props(1, 0, 50);
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
        log.info("输入: holder 占满容量(并发={},队列={},容量={})，目标标识 rb-1 在高峰中被挡回，高峰过后同标识重提",
                p.getConcurrency(), p.getQueueCapacity(), p.getConcurrency() + p.getQueueCapacity());
        FileBackedIdempotencyStore store1 = newStore(p);
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, store1, blocking)) {
            CompletableFuture<List<RecordResult>> holder = CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("rb-hold", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            List<RecordResult> rejected = engine.submitBatch(List.of(new BatchRecord("rb-1", "ok")));
            log.info("判定依据: 容量满时 rb-1 应 REJECTED/backpressure, 实际={}", rejected.get(0));
            assertEquals(RecordStatus.REJECTED, rejected.get(0).status());
            assertTrue(rejected.get(0).reason().contains("backpressure"));

            // 只读查询立刻可见占位已回收：rb-1 如同从未提交（NOT_FOUND），不是在途、更不是已确认
            RecordSnapshot during = engine.lookup("rb-1");
            log.info("判定依据: 被挡回后立即查询 rb-1={}, 应为 NOT_FOUND（占位已回收、不卡死）", during.state());
            assertEquals(SubmissionState.NOT_FOUND, during.state());

            // 高峰未过前再挡一次，同样可回收
            assertEquals(RecordStatus.REJECTED,
                    engine.submitBatch(List.of(new BatchRecord("rb-1", "ok"))).get(0).status());
            assertEquals(SubmissionState.NOT_FOUND, engine.lookup("rb-1").state());

            release.countDown();
            assertEquals(RecordStatus.PROCESSED, holder.join().get(0).status());
        }

        // 高峰过去、服务重启：用同一标识重新提交，必须被当成新任务正常受理并给出真实结论
        try (BatchPipelineEngine engine2 = new BatchPipelineEngine(p, newStore(p), r -> { })) {
            RecordSnapshot before = engine2.lookup("rb-1");
            log.info("判定依据: 重启后查询 rb-1={}, 应为 NOT_FOUND（拒绝记录已回收）", before.state());
            assertEquals(SubmissionState.NOT_FOUND, before.state());

            List<RecordResult> retried = engine2.submitBatch(List.of(new BatchRecord("rb-1", "ok")));
            log.info("判定依据: 同标识重提结果={}, 应为 PROCESSED 而非 DUPLICATE", retried.get(0));
            assertEquals(RecordStatus.PROCESSED, retried.get(0).status());

            // 有了最终结论后，再提交才算重复
            List<RecordResult> dup = engine2.submitBatch(List.of(new BatchRecord("rb-1", "ok")));
            log.info("判定依据: 终态后再提交={}, 应为 DUPLICATE/already confirmed", dup.get(0));
            assertEquals(RecordStatus.DUPLICATE, dup.get(0).status());
            assertTrue(dup.get(0).reason().contains("already confirmed"));
        }
    }

    @Test
    void repeatedRejection_internalOccupancyDoesNotGrow() throws Exception {
        PipelineProperties p = props(1, 0, 20);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            entered.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        int waves = 200;
        log.info("输入: 容量=1 被占满时，同一 businessId 反复冲击 {} 次; 并发={},队列={},超时={}ms",
                waves, p.getConcurrency(), p.getQueueCapacity(), p.getOfferTimeoutMs());
        FileBackedIdempotencyStore store = newStore(p);
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, store, blocking)) {
            CompletableFuture<?> first = CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("occ-holder", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            long rejected = 0;
            for (int i = 0; i < waves; i++) {
                RecordResult r = engine.submitBatch(List.of(new BatchRecord("occ-" + i, "ok"))).get(0);
                assertEquals(RecordStatus.REJECTED, r.status());
                rejected++;
            }
            log.info("判定依据: {} 次冲击全部 REJECTED; 被拒后在途数={} (仅 holder=1), 中断数={}, 终态数={}, 存储文件行数应不含拒绝痕迹",
                    rejected, store.inFlightCount(), store.interruptedCount(), store.count());
            assertEquals(waves, rejected);
            assertEquals(1, store.inFlightCount(), "只有真正受理的 holder 在途");
            assertEquals(0, store.interruptedCount(), "容量拒绝不产生中断记录");
            assertEquals(0, store.count(), "容量拒绝不产生终态记录");
            long lines = Files.readAllLines(Path.of(p.getStoreFile()), StandardCharsets.UTF_8)
                    .stream().filter(l -> !l.isBlank()).count();
            assertEquals(1, lines, "只有 holder 的一条 C(开始) 日志，拒绝不留痕");

            release.countDown();
            first.join();
        }
        // 收尾后再回放：占用全部收敛，文件中恰好 1 条 C + 1 条 R
        FileBackedIdempotencyStore reloaded = newStore(p);
        List<String> lines = Files.readAllLines(Path.of(p.getStoreFile()), StandardCharsets.UTF_8);
        long cLines = lines.stream().filter(l -> l.startsWith("C\t")).count();
        long rLines = lines.stream().filter(l -> l.startsWith("R\t")).count();
        log.info("判定依据: 收敛后文件行数: C={}, R={}; 回放终态数={}, 在途={}, 中断={}",
                cLines, rLines, reloaded.count(), reloaded.inFlightCount(), reloaded.interruptedCount());
        assertEquals(1, cLines);
        assertEquals(1, rLines);
        assertEquals(1, reloaded.count());
        assertEquals(0, reloaded.interruptedCount());
        assertEquals(0, reloaded.inFlightCount());
    }

    @Test
    void restart_confirmedConclusionsStayQueryableAndDeduped() {
        PipelineProperties p = props(2, 8, 500);
        log.info("输入: 重启前落 1 成功 + 1 可重试耗尽失败 + 1 不可恢复失败; 并发={},队列={}",
                p.getConcurrency(), p.getQueueCapacity());
        RecordProcessor failing = r -> {
            if (r.payload().startsWith("RETRY")) {
                throw new com.github.highcumontoa.pipeline.core.RetryableProcessingException("transient");
            }
            if (r.payload().startsWith("FATAL")) {
                throw new com.github.highcumontoa.pipeline.core.FatalProcessingException("permanent");
            }
        };
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, newStore(p), failing)) {
            engine.submitBatch(List.of(
                    new BatchRecord("rc-ok", "ok"),
                    new BatchRecord("rc-retry", "RETRY"),
                    new BatchRecord("rc-fatal", "FATAL")));
        }
        // 模拟重启：新建存储回放 + 新引擎
        FileBackedIdempotencyStore reloaded = newStore(p);
        try (BatchPipelineEngine engine2 = new BatchPipelineEngine(p, reloaded, failing)) {
            RecordSnapshot ok = engine2.lookup("rc-ok");
            RecordSnapshot retry = engine2.lookup("rc-retry");
            RecordSnapshot fatal = engine2.lookup("rc-fatal");
            RecordSnapshot none = engine2.lookup("rc-missing");
            log.info("判定依据: 重启后查询 ok={}/{}, retry={}/{}, fatal={}/{}, 未提交={}",
                    ok.state(), ok.confirmedStatus(), retry.state(), retry.confirmedStatus(),
                    fatal.state(), fatal.confirmedStatus(), none.state());
            assertEquals(SubmissionState.CONFIRMED, ok.state());
            assertEquals(RecordStatus.PROCESSED, ok.confirmedStatus());
            assertEquals(SubmissionState.CONFIRMED, retry.state());
            assertEquals(RecordStatus.FAILED_RETRYABLE, retry.confirmedStatus());
            assertEquals(SubmissionState.CONFIRMED, fatal.state());
            assertEquals(RecordStatus.FAILED_FATAL, fatal.confirmedStatus());
            assertEquals(SubmissionState.NOT_FOUND, none.state());

            // 已确认标识重新提交：一律 DUPLICATE，不会产生第二份结论
            List<RecordResult> dup = engine2.submitBatch(List.of(
                    new BatchRecord("rc-ok", "ok"),
                    new BatchRecord("rc-retry", "RETRY")));
            log.info("判定依据: 重启后重提已确认标识={}, 应全部 DUPLICATE/already confirmed", dup);
            assertEquals(RecordStatus.DUPLICATE, dup.get(0).status());
            assertEquals(RecordStatus.DUPLICATE, dup.get(1).status());
            assertTrue(dup.get(0).reason().contains("already confirmed"));
            assertEquals(3, reloaded.count(), "终态结论一条不增、一条不减");
        }
    }

    @Test
    void interruptedInFlight_restartShowsInterruptedAndResubmitSettlesOnce() throws Exception {
        PipelineProperties p = props(1, 0, 500);
        CountDownLatch entered = new CountDownLatch(1);
        // 永久阻塞闸：不 countDown。被"杀死"引擎的守护工作线程会一直挂在此处，
        // 等价于真实进程被杀（线程随 JVM 退出，永远不会补写终态）。
        CountDownLatch never = new CountDownLatch(1);
        RecordProcessor blocking = r -> {
            entered.countDown();
            while (true) {
                try {
                    // 即便被中断也继续挂住：模拟被杀进程的工作线程不可能在死后补写结论。
                    never.await();
                } catch (InterruptedException ignored) {
                    // 继续等待
                }
            }
        };
        log.info("输入: 1 条记录处理途中模拟强杀（不调用 close、不等待）; 并发={},队列={}",
                p.getConcurrency(), p.getQueueCapacity());
        BatchPipelineEngine killed = new BatchPipelineEngine(p, newStore(p), blocking);
        CompletableFuture<?> hanging = CompletableFuture.supplyAsync(() ->
                killed.submitBatch(List.of(new BatchRecord("kill-1", "ok"))));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        log.info("判定依据: 服务中断。文件中应只有 C(开始) 无 R(终态)");

        FileBackedIdempotencyStore reloaded = newStore(p);
        log.info("判定依据: 回放后中断数={} (应=1), 终态数={} (应=0)",
                reloaded.interruptedCount(), reloaded.count());
        assertEquals(1, reloaded.interruptedCount());
        assertEquals(0, reloaded.count());

        try (BatchPipelineEngine engine2 = new BatchPipelineEngine(p, reloaded, r -> { })) {
            RecordSnapshot snap = engine2.lookup("kill-1");
            log.info("判定依据: 重启后查询={}, 应为 INTERRUPTED 且 confirmedStatus=null（系统不伪造结论）", snap.state());
            assertEquals(SubmissionState.INTERRUPTED, snap.state());
            assertNull(snap.confirmedStatus());

            List<RecordResult> retried = engine2.submitBatch(List.of(new BatchRecord("kill-1", "ok")));
            log.info("判定依据: 同标识重新提交={}, 应被正常受理为 PROCESSED，而不是 DUPLICATE", retried.get(0));
            assertEquals(RecordStatus.PROCESSED, retried.get(0).status());

            RecordSnapshot after = engine2.lookup("kill-1");
            assertEquals(SubmissionState.CONFIRMED, after.state());
            assertEquals(RecordStatus.PROCESSED, after.confirmedStatus());

            // 同一标识在整个生命周期里只能落一份终态结论：再提交必须去重
            List<RecordResult> dup = engine2.submitBatch(List.of(new BatchRecord("kill-1", "ok")));
            assertEquals(RecordStatus.DUPLICATE, dup.get(0).status());
        }

        List<String> lines = Files.readAllLines(Path.of(p.getStoreFile()), StandardCharsets.UTF_8);
        long rForId = lines.stream().filter(l -> l.startsWith("R\tkill-1\t")).count();
        long cForId = lines.stream().filter(l -> l.equals("C\tkill-1")).count();
        log.info("判定依据: 文件中 kill-1 的 C 行={}, R 行={} (终态结论必须恰好 1 份，不丢不重)", cForId, rForId);
        assertEquals(1, rForId, "同一标识不得落两份结论");
        assertEquals(2, cForId, "两次尝试各有一条开始日志");
        hanging.cancel(true);
    }

    @Test
    void lookupIsReadOnly_doesNotConsumeCapacityOrChangeState() throws Exception {
        PipelineProperties p = props(1, 0, 50);
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
        log.info("输入: 容量=1 占满时对在途/未知标识各做多次只读查询; 并发={},队列={}",
                p.getConcurrency(), p.getQueueCapacity());
        FileBackedIdempotencyStore store = newStore(p);
        try (BatchPipelineEngine engine = new BatchPipelineEngine(p, store, blocking)) {
            CompletableFuture<?> first = CompletableFuture.supplyAsync(() ->
                    engine.submitBatch(List.of(new BatchRecord("ro-1", "ok"))));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 10; i++) {
                assertEquals(SubmissionState.IN_FLIGHT, engine.lookup("ro-1").state());
                assertEquals(SubmissionState.NOT_FOUND, engine.lookup("ro-x").state());
            }
            log.info("判定依据: 20 次只读查询后 在途数={} (应=1), 中断数={}, 终态数={}",
                    store.inFlightCount(), store.interruptedCount(), store.count());
            assertEquals(1, store.inFlightCount());
            assertEquals(0, store.interruptedCount());
            assertEquals(0, store.count());
            // 容量仍然被真实任务占满：新提交依旧被背压拒绝，证明查询没占许可
            RecordResult r = engine.submitBatch(List.of(new BatchRecord("ro-2", "ok"))).get(0);
            assertEquals(RecordStatus.REJECTED, r.status());
            release.countDown();
            first.join();
        }
    }
}
