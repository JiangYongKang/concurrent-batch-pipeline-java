package com.github.highcumontoa.concurrentbatchpipelinejava.domain;

import java.util.List;
import java.util.Map;

/**
 * 批量提交的响应：逐条结果 + 按状态聚合计数。
 */
public record BatchSummary(int total, Map<RecordStatus, Long> counts, List<RecordResult> results) {
}
