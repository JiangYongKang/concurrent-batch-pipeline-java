package com.github.highcumontoa.concurrentbatchpipelinejava.domain;

/**
 * 每条记录的最终/中间状态。终态之间互斥，统计口径按终态守恒。
 */
public enum RecordStatus {
    /** 已受理，正在处理中（非终态）。 */
    IN_PROGRESS,
    /** 处理成功（终态）。 */
    SUCCESS,
    /** 业务标识已存在，被幂等去重（终态，不产生重复副作用）。 */
    DUPLICATE,
    /** 并发额度与等待时限内无法承接，被背压拒绝（终态，可稍后重试提交）。 */
    REJECTED_CAPACITY,
    /** 可重试失败且重试次数耗尽，进入死信（终态）。 */
    DEAD_LETTER,
    /** 不可恢复失败（终态，不重试）。 */
    FAILED_PERMANENT
}
