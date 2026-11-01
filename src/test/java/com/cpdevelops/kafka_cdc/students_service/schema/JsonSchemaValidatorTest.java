package com.cpdevelops.kafka_cdc.students_service.schema;

import com.cpdevelops.kafka_cdc.students_service.exceptions.SchemaValidationException;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSchemaValidatorTest {

    private final JsonSchemaValidator validator = new JsonSchemaValidator(new DefaultResourceLoader());

    private static final String VALID_ROW = """
            {"id": 1, "student_id": "stu-1", "course_id": "CS101", "assignment_id": "hw-1",
             "score": 88.5, "status": "GRADED", "submitted_at": "2026-10-31T14:05:12.123456Z"}
            """;

    @Test
    void acceptsAValidRow() {
        assertThatCode(() -> validator.validate("submission-row-v1", VALID_ROW))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsAColumnTheSchemaDoesNotKnow() {
        String rowWithNewColumn = VALID_ROW.replace("\"id\": 1,", "\"id\": 1, \"late_minutes\": 5,");

        assertThatThrownBy(() -> validator.validate("submission-row-v1", rowWithNewColumn))
                .isInstanceOf(SchemaValidationException.class)
                .hasMessageContaining("submission-row-v1");
    }

    @Test
    void rejectsAWrongType() {
        String rowWithTextScore = VALID_ROW.replace("88.5", "\"high\"");

        assertThatThrownBy(() -> validator.validate("submission-row-v1", rowWithTextScore))
                .isInstanceOf(SchemaValidationException.class);
    }
}