package com.cpdevelops.kafka_cdc.students_service.event;


import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SubmissionEventMapperTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final SubmissionEventMapper mapper = new SubmissionEventMapper();

    private static final String UPDATE_ENVELOPE = """
            {
              "before": {"id": 21, "student_id": "stu-20", "course_id": "CS101", "assignment_id": "hw-3",
                         "score": null, "status": "SUBMITTED", "submitted_at": "2026-10-31T14:05:12Z"},
              "after":  {"id": 21, "student_id": "stu-20", "course_id": "CS101", "assignment_id": "hw-3",
                         "score": 87.0, "status": "GRADED", "submitted_at": "2026-10-31T14:05:12Z"},
              "source": {"lsn": 26853472, "ts_ms": 1793113445000},
              "op": "u"
            }
            """;

    @Test
    void mapsAnUpdateToACleanEvent() {
        DebeziumChangeEvent change = DebeziumChangeEvent.from(jsonMapper.readTree(UPDATE_ENVELOPE));

        SubmissionEvent event = mapper.toEvent(change, change.rowState());

        assertThat(event.changeType()).isEqualTo(ChangeType.UPDATED);
        assertThat(event.submissionId()).isEqualTo(21);
        assertThat(event.score()).isEqualByComparingTo(new BigDecimal("87"));
        assertThat(event.status()).isEqualTo("GRADED");
        assertThat(event.occurredAt()).isEqualTo(Instant.ofEpochMilli(1793113445000L));
    }

    @Test
    void producesTheSameEventIdForTheSameChange() {
        DebeziumChangeEvent change = DebeziumChangeEvent.from(jsonMapper.readTree(UPDATE_ENVELOPE));

        String first = mapper.toEvent(change, change.rowState()).eventId();
        String second = mapper.toEvent(change, change.rowState()).eventId();

        assertThat(first).isEqualTo("21-26853472-u").isEqualTo(second);
    }
}