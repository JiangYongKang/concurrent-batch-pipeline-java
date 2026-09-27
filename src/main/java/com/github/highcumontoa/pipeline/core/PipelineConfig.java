package com.github.highcumontoa.pipeline.core;

import com.github.highcumontoa.pipeline.store.IdempotencyStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    /**
     * 默认业务处理逻辑（演示用）：
     * payload 以 "FATAL" 开头 → 不可恢复失败；以 "RETRY" 开头 → 可重试失败；
     * 其余视为成功。真实场景替换为实际业务 Bean 即可。
     */
    @Bean
    public RecordProcessor recordProcessor() {
        return record -> {
            String payload = record.payload() == null ? "" : record.payload();
            if (payload.startsWith("FATAL")) {
                throw new FatalProcessingException("payload rejected by business rule");
            }
            if (payload.startsWith("RETRY")) {
                throw new RetryableProcessingException("transient downstream error");
            }
        };
    }

    @Bean(destroyMethod = "close")
    public BatchPipelineEngine batchPipelineEngine(PipelineProperties properties,
                                                   IdempotencyStore store,
                                                   RecordProcessor processor) {
        return new BatchPipelineEngine(properties, store, processor);
    }
}
