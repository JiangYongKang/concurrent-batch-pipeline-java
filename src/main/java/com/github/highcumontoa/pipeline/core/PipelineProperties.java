package com.github.highcumontoa.pipeline.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 并发与背压相关配置项。 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    /** 处理线程并发度。 */
    private int concurrency = 4;

    /** 等待队列容量（有界）。 */
    private int queueCapacity = 64;

    /** 提交时等待空位的最长毫秒数，超时即背压拒绝。 */
    private long offerTimeoutMs = 200;

    /** 可重试失败的最大尝试次数（含首次）。 */
    private int maxAttempts = 3;

    /** 关闭时等待在途任务收敛的最长毫秒数。 */
    private long shutdownTimeoutMs = 10_000;

    /** 幂等结果存储文件路径。 */
    private String storeFile = "data/idempotency-store.log";

    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public long getOfferTimeoutMs() { return offerTimeoutMs; }
    public void setOfferTimeoutMs(long offerTimeoutMs) { this.offerTimeoutMs = offerTimeoutMs; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getShutdownTimeoutMs() { return shutdownTimeoutMs; }
    public void setShutdownTimeoutMs(long shutdownTimeoutMs) { this.shutdownTimeoutMs = shutdownTimeoutMs; }
    public String getStoreFile() { return storeFile; }
    public void setStoreFile(String storeFile) { this.storeFile = storeFile; }
}
