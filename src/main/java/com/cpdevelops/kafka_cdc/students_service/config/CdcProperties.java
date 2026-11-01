package com.cpdevelops.kafka_cdc.students_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.cdc")
public record CdcProperties(
        String sourceTopic,
        String eventsTopic,
        String rowSchema
) {
}
