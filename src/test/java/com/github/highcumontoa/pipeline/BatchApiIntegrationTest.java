package com.github.highcumontoa.pipeline;

import com.github.highcumontoa.pipeline.core.BatchRecord;
import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordStatus;
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
}
