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

    public CdcProperties {
        if (retry == null) {
            retry = new Retry(3, Duration.ofMillis(500), 2.0, Duration.ofSeconds(5));
        }
        if (failureSimulation == null) {
            failureSimulation = new FailureSimulation(false, Duration.ZERO);
        }
    }

    public record Retry(
            int maxRetries,
            Duration initialInterval,
            double multiplier,
            Duration maxInterval) {
    }

    public record FailureSimulation(boolean enabled, Duration processingDelay) {
        public FailureSimulation {
            if (processingDelay == null) {
                processingDelay = Duration.ZERO;
            }
        }
    }
}