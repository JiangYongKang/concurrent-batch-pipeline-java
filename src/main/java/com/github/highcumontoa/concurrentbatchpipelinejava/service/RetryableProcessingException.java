package com.github.highcumontoa.concurrentbatchpipelinejava.service;

/** 可重试失败：处理逻辑抛出该异常时按 maxRetries 重试。 */
public class RetryableProcessingException extends RuntimeException {
    public RetryableProcessingException(String message) {
        super(message);
    }
}
