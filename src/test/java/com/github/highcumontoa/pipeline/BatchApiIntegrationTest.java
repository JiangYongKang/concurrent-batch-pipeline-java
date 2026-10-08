package com.github.highcumontoa.pipeline;

import com.github.highcumontoa.pipeline.core.BatchRecord;
import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import com.github.highcumontoa.pipeline.core.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BatchApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(BatchApiIntegrationTest.class);

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("pipeline.store-file", () -> tempDir.resolve("api-store.log").toString());
        registry.add("pipeline.concurrency", () -> "2");
        registry.add("pipeline.queue-capacity", () -> "8");
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Test
    void submitBatchViaApi_returnsPerRecordResultsAndConservedStats() {
        List<BatchRecord> batch = List.of(
                new BatchRecord("api-1", "ok"),
                new BatchRecord("api-2", "FATAL boom"),
                new BatchRecord("api-1", "ok"));
        log.info("输入: {} 条记录(含 1 条重复、1 条不可恢复失败), 并发度=2", batch.size());
        RecordResult[] results = rest.postForObject("/api/batch", batch, RecordResult[].class);
        log.info("判定依据: 逐条结果={}", List.of(results));
        assertEquals(RecordStatus.PROCESSED, results[0].status());
        assertEquals(RecordStatus.FAILED_FATAL, results[1].status());
        assertEquals(RecordStatus.DUPLICATE, results[2].status());

        Map<?, ?> stats = rest.getForObject("/api/batch/stats", Map.class);
        log.info("判定依据: stats={}, conserved 应为 true", stats);
        assertEquals(Boolean.TRUE, stats.get("conserved"));
        assertEquals(3, ((Number) stats.get("submitted")).intValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusEndpoint_distinguishesStatesAndIsReadOnly() {
        log.info("输入: 经 HTTP 验证状态查询四态; 并发度=2, 队列=8");
        // 未提交过 → UNKNOWN
        Map<String, Object> unknown = rest.getForObject("/api/batch/status/api-never", Map.class);
        log.info("判定依据: 未提交标识查询={}（state 应为 UNKNOWN）", unknown);
        assertEquals(TaskState.UNKNOWN.name(), unknown.get("state"));

        // 提交一条成功、一条失败
        rest.postForObject("/api/batch",
                List.of(new BatchRecord("api-ok", "ok"),
                        new BatchRecord("api-bad", "FATAL boom")),
                RecordResult[].class);

        Map<String, Object> succeeded = rest.getForObject("/api/batch/status/api-ok", Map.class);
        Map<String, Object> failed = rest.getForObject("/api/batch/status/api-bad", Map.class);
        log.info("判定依据: 成功标识查询={}, 失败标识查询={}", succeeded, failed);
        assertEquals(TaskState.SUCCEEDED.name(), succeeded.get("state"));
        assertEquals(RecordStatus.PROCESSED.name(), succeeded.get("recordStatus"));
        assertEquals(TaskState.FAILED.name(), failed.get("state"));
        assertEquals(RecordStatus.FAILED_FATAL.name(), failed.get("recordStatus"));

        // 只读：连续查询结果不变；重复提交仍被去重，结论明细不被覆盖
        Map<String, Object> succeededAgain = rest.getForObject("/api/batch/status/api-ok", Map.class);
        RecordResult[] dup = rest.postForObject("/api/batch",
                new BatchRecord[]{new BatchRecord("api-ok", "ok")}, RecordResult[].class);
        Map<String, Object> afterDup = rest.getForObject("/api/batch/status/api-ok", Map.class);
        log.info("判定依据: 再查={}, 重复提交={}, 查后仍={}（查询/重复提交均不改变结论）",
                succeededAgain, dup[0], afterDup);
        assertEquals(RecordStatus.DUPLICATE, dup[0].status());
        assertEquals(succeeded, succeededAgain);
        assertEquals(succeeded, afterDup);
    }
}
