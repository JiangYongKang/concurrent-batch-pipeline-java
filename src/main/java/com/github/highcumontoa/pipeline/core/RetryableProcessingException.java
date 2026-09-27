package com.github.highcumontoa.pipeline.core;

/** 可重试失败：处理逻辑抛出该异常时引擎会按上限重试。 */
public class RetryableProcessingException extends RuntimeException {
    public RetryableProcessingException(String message) {
        super(message);
    }
}
