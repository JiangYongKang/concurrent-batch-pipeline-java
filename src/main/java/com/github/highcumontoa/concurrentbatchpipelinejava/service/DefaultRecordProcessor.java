package com.github.highcumontoa.concurrentbatchpipelinejava.service;

import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchRecord;

/**
 * 默认业务处理器（演示/本地验证用）：
 * <ul>
 *   <li>payload 以 {@code RETRYABLE:} 开头 —— 抛可重试异常；</li>
 *   <li>payload 以 {@code PERMANENT:} 开头 —— 抛不可恢复异常；</li>
 *   <li>其他 —— 视为处理成功。</li>
 * </ul>
 * 生产环境以自定义 {@link RecordProcessor} Bean 覆盖即可（见 PipelineConfig 的条件装配）。
 */
public class DefaultRecordProcessor implements RecordProcessor {

    @Override
    public void process(BatchRecord record) {
        String payload = record.payload() == null ? "" : record.payload();
        if (payload.startsWith("RETRYABLE:")) {
            throw new RetryableProcessingException(payload.substring("RETRYABLE:".length()));
        }
        if (payload.startsWith("PERMANENT:")) {
            throw new PermanentProcessingException(payload.substring("PERMANENT:".length()));
        }
    }
}
