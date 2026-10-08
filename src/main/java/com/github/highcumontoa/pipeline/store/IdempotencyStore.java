package com.github.highcumontoa.pipeline.store;

import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordSnapshot;
import com.github.highcumontoa.pipeline.core.RecordStatus;

import java.util.Optional;

/**
 * 幂等结果存储。
 *
 * <p>生命周期语义：
 * <ul>
 *   <li>{@link #acquireClaim} 原子地把一个标识纳入本次受理：已确认 / 在途则去重，
 *       从未见过或上次被中断则受理成功；</li>
 *   <li>容量不足等"尚未开始处理"的受理失败必须 {@link #rollbackClaim} 回滚，
 *       使标识恢复到可重新受理的状态，内部占用不膨胀；</li>
 *   <li>真正开始处理前由 {@link #persistAttemptStarted} 写一条前置日志（WAL），
 *       保证"处理到一半被中断"在重启后可被识别为 INTERRUPTED，而不是被当成已处理；</li>
 *   <li>终态结果先落盘再可见（{@link #saveResult}），重启后已确认结论不丢、不重复。</li>
 * </ul>
 */
public interface IdempotencyStore {

    /** acquireClaim 的结果。 */
    enum ClaimOutcome {
        /** 受理成功：标识已占位为在途，调用方最终必须 saveResult / rollbackClaim / markInterrupted 之一。 */
        ACCEPTED,
        /** 已有最终结论。 */
        ALREADY_CONFIRMED,
        /** 当前进程内已有同一标识正在处理。 */
        ALREADY_IN_FLIGHT
    }

    /**
     * 原子尝试受理某 businessId。
     *
     * @return {@link ClaimOutcome#ACCEPTED} 表示已占位成功（首次、或此前仅被容量拒绝、
     *         或重启前在途被中断）；其余为去重，且不改变任何状态。
     */
    ClaimOutcome acquireClaim(String businessId);

    /**
     * 回滚本次受理占位（用于容量满被挡回、引擎关闭中等"尚未开始处理"的情况）。
     * 调用后该标识回到与受理前一致的可受理状态（NOT_FOUND 或 INTERRUPTED）。
     */
    void rollbackClaim(String businessId);

    /**
     * 在真正执行业务处理前记录"处理已开始"（先写日志再更新内存）。
     * 若之后没有终态落盘，重启回放时该标识呈现为 INTERRUPTED。
     */
    void persistAttemptStarted(String businessId);

    /**
     * 把在途占位标记为中断未决（如提交前置日志失败），状态转为 INTERRUPTED。
     */
    void markInterrupted(String businessId);

    /** 持久化一条终态结果（先 append+flush，再对外可见），并清退在途占位。 */
    void saveResult(RecordResult result);

    /** 查询已有终态结果（只读）。 */
    Optional<RecordResult> find(String businessId);

    /** 按业务标识查询当前生命周期快照（只读，不改变状态、不占处理能力）。 */
    RecordSnapshot lookup(String businessId);

    /** 当前已确认的终态结果数（只增不减）。 */
    long count();

    /** 当前进程内正在处理（已受理未出结论）的标识数。 */
    long inFlightCount();

    /** 回放/运行中识别出的"处理已开始但无终态"（中断未决）标识数。 */
    long interruptedCount();

    /** 已确认终态是否为给定状态（便于引擎组装去重原因）。 */
    default boolean isConfirmedWithStatus(String businessId, RecordStatus status) {
        return find(businessId).map(r -> r.status() == status).orElse(false);
    }
}
