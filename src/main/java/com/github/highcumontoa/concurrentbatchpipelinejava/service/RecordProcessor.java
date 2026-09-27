package com.github.highcumontoa.concurrentbatchpipelinejava.service;

import com.github.highcumontoa.concurrentbatchpipelinejava.domain.BatchRecord;

/**
 * 业务处理器。实现方通过抛出 {@link RetryableProcessingException} /
 * {@link PermanentProcessingException} 表达失败分类；其他异常按不可恢复处理。
 */
public interface RecordProcessor {
    void process(BatchRecord record);
}
