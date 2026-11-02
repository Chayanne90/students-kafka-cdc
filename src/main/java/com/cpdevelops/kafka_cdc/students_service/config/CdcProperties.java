package com.cpdevelops.kafka_cdc.students_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "app.cdc")
public record CdcProperties(
        String sourceTopic,
        String eventsTopic,
        String deadLetterTopic,
        String rowSchema,
        Retry retry,
        FailureSimulation failureSimulation) {

    public record Retry(
            int maxRetries,
            Duration initialInterval,
            double multiplier,
            Duration maxInterval) {
    }

    public record FailureSimulation(boolean enabled) {
    }
}