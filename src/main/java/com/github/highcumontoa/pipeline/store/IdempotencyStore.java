package com.github.highcumontoa.pipeline.store;

import com.github.highcumontoa.pipeline.core.RecordResult;

import java.util.Optional;

/** 幂等结果存储：保证同一 businessId 不产生重复副作用，重启后已确认结果不丢失。 */
public interface IdempotencyStore {

    /**
     * 尝试为 businessId 占位（进入处理中）。
     *
     * @return true 表示占位成功（首次见到该标识）；false 表示已存在（重复提交）
     */
    boolean tryClaim(String businessId);

    /** 持久化一条终态结果。 */
    void saveResult(RecordResult result);

    /** 查询已有结果。 */
    Optional<RecordResult> find(String businessId);

    /** 当前已确认的终态结果数。 */
    long count();
}
