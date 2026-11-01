package com.cpdevelops.kafka_cdc.students_service.event;

import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import tools.jackson.databind.JsonNode;

import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.time.Instant;

@Component
public class SubmissionEventMapper {

    public SubmissionEvent toEvent(DebeziumChangeEvent change, JsonNode row) {

        long submissionId = row.get("id").asLong();

        return new SubmissionEvent(
                eventId( submissionId, change),
                SubmissionEvent.CURRENT_SCHEMA_VERSION,
                ChangeType.fromDebezium(change.op()),
                submissionId,
                row.get("student_id").asString(),
                row.get("course_id").asString(),
                row.get("assigment_id").asString(),
                row.hasNonNull("score") ? BigDecimal.valueOf(row.get("score").asDouble()) : null,
                row.get("status").asString(),
                Instant.parse(row.get("submitted_at").asString()),
                Instant.ofEpochMilli(change.sourceTimestamps()));
    }

    /**
     * The same database change always produces the same eventId,
     * so consumers can detect duplicates when events are replayed.
     */
    private static String eventId(long submissionId, DebeziumChangeEvent change) {
        return "%d-%d-%s".formatted(submissionId, change.lsn(), change.op());
    }


}
