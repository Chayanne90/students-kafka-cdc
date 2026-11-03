package com.cpdevelops.kafka_cdc.students_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "app.analytics")
public record AnalyticsProperties(
        String courseActivityTopic,
        String courseGradeStatsTopic,
        Duration activityWindow,
        Duration activityGrace
) {

}
