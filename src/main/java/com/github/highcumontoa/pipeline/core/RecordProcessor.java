package com.github.highcumontoa.pipeline.core;

/** 业务处理逻辑接口，由使用方实现。 */
public interface RecordProcessor {
    void process(BatchRecord record) throws RetryableProcessingException, FatalProcessingException;
}
