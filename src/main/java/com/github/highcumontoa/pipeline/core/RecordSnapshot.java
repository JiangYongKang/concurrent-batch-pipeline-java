package com.github.highcumontoa.pipeline.core;

/**
 * 按业务标识查询到的只读快照。
 */
public record RecordSnapshot(
        String businessId,
        SubmissionState state,
        RecordStatus confirmedStatus,
        String reason,
        int attempts
) {
}
