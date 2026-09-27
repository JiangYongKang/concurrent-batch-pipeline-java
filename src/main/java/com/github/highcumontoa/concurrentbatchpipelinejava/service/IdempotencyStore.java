package com.github.highcumontoa.concurrentbatchpipelinejava.service;

import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordResult;

import java.util.Optional;

/**
 * 幂等存储：以业务标识为键，putIfAbsent 保证同一键只受理一次；
 * 终态结果持久化，重启后恢复，避免重复处理与结果丢失。
 */
public interface IdempotencyStore {

    /** 尝试登记一个键为"处理中"。已存在（处理中或已完成）时返回 false。 */
    boolean tryClaim(String businessKey);

    /** 写入终态结果并持久化。仅对已 claim 的键有效。 */
    void complete(String businessKey, RecordResult result);

    /** 查询已有终态结果。 */
    Optional<RecordResult> findResult(String businessKey);

    /** 当前已登记键数量（含处理中）。 */
    int size();
}
