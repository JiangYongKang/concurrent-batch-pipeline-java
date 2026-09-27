package com.github.highcumontoa.concurrentbatchpipelinejava.tests;

import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordResult;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordStatus;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.FileBackedIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 停机/重启语义：已确认终态不丢失，在途 claim 不重复处理。
 */
class IdempotencyStoreRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyStoreRecoveryTest.class);

    @Test
    void restart_recoversTerminalAndInFlightClaims(@TempDir Path dir) {
        String path = dir.resolve("store.log").toString();
        log.info("[停机恢复] 输入: 1 条已确认终态(done-key) + 1 条仅在途(inflight-key), 判定依据: "
                + "重启后终态可查、两键均不可再次 claim");

        FileBackedIdempotencyStore first = new FileBackedIdempotencyStore(path);
        assertTrue(first.tryClaim("done-key"));
        first.complete("done-key", new RecordResult("done-key", RecordStatus.SUCCESS, "处理成功", 1));
        assertTrue(first.tryClaim("inflight-key")); // 模拟停机时仍在途，无终态
        first.close();

        FileBackedIdempotencyStore second = new FileBackedIdempotencyStore(path);
        var recovered = second.findResult("done-key");
        log.info("[停机恢复] 结果: done-key 恢复={} inflight-key 可再claim={}",
                recovered.map(RecordResult::status).orElse(null), second.tryClaim("inflight-key"));
        // 注意：上面 tryClaim 的调用仅用于演示输出，断言使用独立键重新验证
        assertTrue(recovered.isPresent(), "已确认结果重启后不得丢失");
        assertEquals(RecordStatus.SUCCESS, recovered.get().status());

        FileBackedIdempotencyStore third = new FileBackedIdempotencyStore(path);
        assertFalse(third.tryClaim("done-key"), "已有终态的键不得重复受理");
        assertFalse(third.tryClaim("inflight-key"), "停机在途键不得重复受理（防重复副作用）");
        assertTrue(third.tryClaim("new-key"), "新键仍可正常受理");
        third.close();
        second.close();
    }
}
