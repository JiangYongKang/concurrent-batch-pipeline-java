package com.github.highcumontoa.pipeline.core;

/** 一条待处理记录：businessId 为幂等键，payload 为业务内容。 */
public record BatchRecord(String businessId, String payload) {
}
