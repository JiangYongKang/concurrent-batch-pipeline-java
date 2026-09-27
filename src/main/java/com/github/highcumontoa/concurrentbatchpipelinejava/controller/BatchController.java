package com.github.highcumontoa.concurrentbatchpipelinejava.controller;

import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchRecord;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchSummary;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.BatchPipelineService;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.PipelineMetrics;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 批处理提交与指标查询入口。
 */
@RestController
@RequestMapping("/api/batch")
public class BatchController {

    private final BatchPipelineService pipelineService;
    private final PipelineMetrics metrics;

    public BatchController(BatchPipelineService pipelineService, PipelineMetrics metrics) {
        this.pipelineService = pipelineService;
        this.metrics = metrics;
    }

    @PostMapping
    public BatchSummary submit(@RequestBody List<BatchRecord> records) {
        return pipelineService.submit(records);
    }

    @GetMapping("/metrics")
    public Map<String, Long> metrics() {
        return metrics.snapshot();
    }
}
