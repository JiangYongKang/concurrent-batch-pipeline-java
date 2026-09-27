package com.github.highcumontoa.pipeline.core;

/** 单条记录的处理结果，reason 给出可区分的原因说明。 */
public record RecordResult(String businessId, RecordStatus status, String reason, int attempts) {
}
