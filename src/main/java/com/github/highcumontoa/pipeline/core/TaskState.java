package com.github.highcumontoa.pipeline.core;

/**
 * 按业务标识查询到的任务当前所处状态（只读视图）。
 *
 * <p>四类状态互斥，重启前后对同一标识的结论保持一致：
 * <ul>
 *   <li>{@link #SUCCEEDED}：已有最终成功结论（PROCESSED）；</li>
 *   <li>{@link #FAILED}：已有最终失败结论（FAILED_RETRYABLE / FAILED_FATAL）；</li>
 *   <li>{@link #IN_PROGRESS}：已被受理、正在处理，尚无最终结论；</li>
 *   <li>{@link #UNKNOWN}：从未提交过，或曾被背压快速挡回（未形成任何结论，可正常重提）。</li>
 * </ul>
 */
public enum TaskState {
    SUCCEEDED,
    FAILED,
    IN_PROGRESS,
    UNKNOWN
}
