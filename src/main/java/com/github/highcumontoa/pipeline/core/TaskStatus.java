package com.github.highcumontoa.pipeline.core;

/**
 * 按 businessId 查询到的任务状态快照（只读）。
 *
 * <p>查询不改变任何状态、不占用处理容量。state 为粗粒度四态；
 * 对已有最终结论的标识，recordStatus / reason / attempts 回填该结论的明细；
 * IN_PROGRESS 与 UNKNOWN 时明细字段为空。
 *
 * @param businessId   被查询的业务标识
 * @param state        四态结论：SUCCEEDED / FAILED / IN_PROGRESS / UNKNOWN
 * @param recordStatus 最终落盘的明细状态（PROCESSED / FAILED_*），无最终结论时为 null
 * @param reason       最终结论附带的原因说明，无最终结论时为 null
 * @param attempts     最终结论实际使用的尝试次数，无最终结论时为 0
 */
public record TaskStatus(String businessId,
                         TaskState state,
                         RecordStatus recordStatus,
                         String reason,
                         int attempts) {
}
