package com.github.highcumontoa.concurrentbatchpipelinejava.domain;

/**
 * 单条记录的处理结果反馈。
 *
 * @param businessKey 业务标识
 * @param status      最终状态
 * @param reason      可区分的原因说明（去重 / 背压 / 失败分类）
 * @param attempts    实际执行次数（含重试）
 */
public record RecordResult(String businessKey, RecordStatus status, String reason, int attempts) {
}
