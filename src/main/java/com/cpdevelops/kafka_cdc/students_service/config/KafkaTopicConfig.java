package com.cpdevelops.kafka_cdc.students_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic submissionEventsTopic(CdcProperties properties) {
        return TopicBuilder.name(properties.eventsTopic())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
