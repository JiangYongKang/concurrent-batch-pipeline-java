package com.github.highcumontoa.pipeline.store;

import com.github.highcumontoa.pipeline.core.RecordResult;

import java.util.Optional;

/**
 * 幂等结果存储：保证同一 businessId 不产生重复副作用，重启后已确认结果不丢失。
 *
 * <p>标识生命周期：
 * <pre>
 *   (未知) --tryClaim--> IN_PROGRESS（处理中占位）
 *      ├─ saveResult  --> 最终结论（PROCESSED / FAILED_*），落盘、重启不丢
 *      └─ releaseClaim--> 回到(未知)：仅用于"未形成结论"的情形（背压拒绝/关闭竞态），
 *                         之后同一标识可被重新 tryClaim 受理
 * </pre>
 */
public interface IdempotencyStore {

    /**
     * 尝试为 businessId 占位（进入处理中）。
     *
     * @return true 表示占位成功（标识当前无占位且无最终结论）；false 表示已被占位（在途）或已有最终结论
     */
    boolean tryClaim(String businessId);

    /**
     * 释放一次 {@link #tryClaim} 成功、但最终没有形成结论的占位。
     *
     * <p>仅当标识处于"处理中、无最终结论"时生效；已有最终结论的标识不受影响。
     * 用于背压拒绝 / 关闭竞态后回收内部记录，使同一标识可重新提交。
     *
     * @return true 表示确实回收了一个占位；false 表示没有可回收的占位（不应发生）
     */
    boolean releaseClaim(String businessId);

    /** 持久化一条最终结果（先落盘再可见）。已有最终结论时以先到为准，不覆盖。 */
    void saveResult(RecordResult result);

    /** 查询已有最终结果；处理中或未知返回空。 */
    Optional<RecordResult> find(String businessId);

    /** 是否处于"已占位、尚无最终结论"的处理中状态。只读，不改变状态。 */
    boolean isInFlight(String businessId);

    /** 当前已确认的最终结果数（重启后仍然存在）。 */
    long count();

    /**
     * 当前系统内保留的标识占用数（在途占位 + 已确认结论）。
     *
     * <p>用于观测背压拒绝回收是否彻底：被挡回的占位释放后该计数应回落，
     * 长跑下只随真实在途任务与真实结论变化，不随拒绝次数膨胀。
     */
    long claimedCount();
}
