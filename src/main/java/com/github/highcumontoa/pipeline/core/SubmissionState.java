package com.github.highcumontoa.pipeline.core;

/**
 * 按业务标识查询到的提交生命周期状态（只读视图，不含任何处理占用）。
 *
 * <ul>
 *   <li>{@link #CONFIRMED}：已经有最终结论（成功或各类失败），重启后保持一致；</li>
 *   <li>{@link #IN_FLIGHT}：当前进程内已受理、正在处理，尚未出结论；</li>
 *   <li>{@link #INTERRUPTED}：日志中存在"已开始处理"的记录但没有终态，
 *       通常是处理途中服务被中断（崩溃/强杀）所致；重启后对该标识重新提交会被正常受理，
 *       且系统不会伪造任何结论；</li>
 *   <li>{@link #NOT_FOUND}：从未见过该标识，或其提交仅在容量满时被快速挡回（占位已回收）。</li>
 * </ul>
 */
public enum SubmissionState {
    /** 已经确定最终结论（PROCESSED / FAILED_RETRYABLE / FAILED_FATAL）。 */
    CONFIRMED,
    /** 正在处理、还没有最终结论。 */
    IN_FLIGHT,
    /** 曾开始处理但未见终态（疑似中途被中断），结论未定，允许重新提交。 */
    INTERRUPTED,
    /** 从未提交，或仅有过已被回收的容量拒绝。 */
    NOT_FOUND
}
