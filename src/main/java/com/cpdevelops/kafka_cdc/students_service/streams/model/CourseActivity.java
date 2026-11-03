package com.cpdevelops.kafka_cdc.students_service.streams.model;

import java.time.Instant;

public record CourseActivity(
        String courseId,
        Instant windowStart,
        Instant windowEnd,
        long submission) {
}
