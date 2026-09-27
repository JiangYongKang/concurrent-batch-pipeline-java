package com.github.highcumontoa.concurrentbatchpipelinejava.config;

import com.github.highcumontoa.concurrentbatchpipelinejava.service.DefaultRecordProcessor;
import com.github.highcumontoa.concurrentbatchpipelinejava.service.RecordProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    /** 未提供业务处理器时装配默认实现。 */
    @Bean
    @ConditionalOnMissingBean(RecordProcessor.class)
    public RecordProcessor recordProcessor() {
        return new DefaultRecordProcessor();
    }
}
