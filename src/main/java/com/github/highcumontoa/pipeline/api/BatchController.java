package com.github.highcumontoa.pipeline.api;

import com.github.highcumontoa.pipeline.core.BatchPipelineEngine;
import com.github.highcumontoa.pipeline.core.BatchRecord;
import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/batch")
public class BatchController {

    private final BatchPipelineEngine engine;

    public BatchController(BatchPipelineEngine engine) {
        this.engine = engine;
    }

    /** 批量提交，逐条返回最终状态与原因。 */
    @PostMapping
    public List<RecordResult> submit(@RequestBody List<BatchRecord> records) {
        return engine.submitBatch(records);
    }

    /** 状态计数快照与守恒校验（sum(stats) == submitted）。 */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<RecordStatus, Long> stats = engine.stats();
        long sum = stats.values().stream().mapToLong(Long::longValue).sum();
        return Map.of(
                "stats", stats,
                "submitted", engine.submittedCount(),
                "conserved", sum == engine.submittedCount());
    }
}
