package com.github.highcumontoa.pipeline.core;

/** 不可恢复失败：处理逻辑抛出该异常时引擎不重试，直接落终态。 */
public class FatalProcessingException extends RuntimeException {
    public FatalProcessingException(String message) {
        super(message);
    }
}
