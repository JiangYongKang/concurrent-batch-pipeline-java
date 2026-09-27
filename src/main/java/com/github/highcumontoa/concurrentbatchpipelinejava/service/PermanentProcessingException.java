package com.github.highcumontoa.concurrentbatchpipelinejava.service;

/** 不可恢复失败：处理逻辑抛出该异常时不重试，直接置为 FAILED_PERMANENT。 */
public class PermanentProcessingException extends RuntimeException {
    public PermanentProcessingException(String message) {
        super(message);
    }
}
