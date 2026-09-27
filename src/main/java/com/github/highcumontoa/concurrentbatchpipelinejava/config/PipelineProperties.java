package com.github.highcumontoa.concurrentbatchpipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 并发批处理管线配置项。
 *
 * @param concurrency       最大并发处理度（信号量与线程池大小）
 * @param queueCapacity     等待队列容量（有界，超出即背压）
 * @param acquireTimeoutMs  获取处理许可的最大等待毫秒数，超时快速拒绝
 * @param maxRetries        可重试失败的最大重试次数
 * @param shutdownTimeoutMs 停机时等待在途任务收敛的超时毫秒数
 * @param storePath         幂等结果持久化文件路径（重启后恢复，防重复处理）
 */
@ConfigurationProperties(prefix = "pipeline")
public record PipelineProperties(
        int concurrency,
        int queueCapacity,
        long acquireTimeoutMs,
        int maxRetries,
        long shutdownTimeoutMs,
        String storePath) {

    public PipelineProperties {
        if (concurrency <= 0) concurrency = 4;
        if (queueCapacity < 0) queueCapacity = 0;
        if (acquireTimeoutMs < 0) acquireTimeoutMs = 0;
        if (maxRetries < 0) maxRetries = 0;
        if (shutdownTimeoutMs <= 0) shutdownTimeoutMs = 5000;
        if (storePath == null || storePath.isBlank()) storePath = "data/idempotency-store.log";
    }
}
