package com.github.highcumontoa.pipeline.core;

/** 单条记录的最终状态。 */
public enum RecordStatus {
    /** 本次提交被受理并成功处理。 */
    PROCESSED,
    /** 业务标识已存在，命中幂等去重，未产生重复副作用。 */
    DUPLICATE,
    /** 因容量饱和 / 背压被快速拒绝。 */
    REJECTED,
    /** 可重试失败且重试次数已耗尽。 */
    FAILED_RETRYABLE,
    /** 不可恢复失败，不重试。 */
    FAILED_FATAL
}
