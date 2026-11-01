package com.cpdevelops.kafka_cdc.students_service.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SubmissionEvent(
        String eventId,
        int schemaVersion,
        ChangeType changeType,
        long submissionId,
        String studentId,
        String courseId,
        String assignmentId,
        BigDecimal score,
        String status,
        Instant submittedAt,
        Instant occurredAt
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
