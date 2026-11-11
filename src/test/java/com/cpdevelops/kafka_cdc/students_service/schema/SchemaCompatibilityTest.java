package com.cpdevelops.kafka_cdc.students_service.schema;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;


class SchemaCompatibilityTest {

    /** Fields that change on purpose with every version. */
    private static final Set<String> VERSION_FIELDS = Set.of("schemaVersion");

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void eventV2IsBackwardCompatibleWithV1() {
        assertThat(backwardProblems("submission-event-v1", "submission-event-v2")).isEmpty();
    }

    @Test
    void eventV2IsNotForwardCompatibleWithStrictV1Readers() {
        assertThat(forwardProblems("submission-event-v1", "submission-event-v2"))
                .containsExactly("lateMinutes is not allowed by the reader schema");
    }

    @Test
    void rowV2IsBackwardCompatibleWithRowV1() {
        assertThat(backwardProblems("submission-row-v1", "submission-row-v2")).isEmpty();
    }

    private List<String> backwardProblems(String oldSchema, String newSchema) {
        return problemsReading(load(oldSchema), load(newSchema));
    }

    private List<String> forwardProblems(String oldSchema, String newSchema) {
        return problemsReading(load(newSchema), load(oldSchema));
    }

    /** Lists the reasons why data valid under the writer schema could be rejected by the reader schema. */
    @SuppressWarnings("unchecked")
    static List<String> problemsReading(Map<String, Object> writer, Map<String, Object> reader) {
        List<String> problems = new ArrayList<>();

        Map<String, Map<String, Object>> writerProps = (Map<String, Map<String, Object>>) writer.get("properties");
        Map<String, Map<String, Object>> readerProps = (Map<String, Map<String, Object>>) reader.get("properties");
        List<String> writerRequired = (List<String>) writer.getOrDefault("required", List.of());
        List<String> readerRequired = (List<String>) reader.getOrDefault("required", List.of());
        boolean readerIsClosed = Boolean.FALSE.equals(reader.get("additionalProperties"));

        // 1. Every field the writer may produce must be accepted by the reader
        for (Map.Entry<String, Map<String, Object>> entry : writerProps.entrySet()) {
            String field = entry.getKey();
            if (VERSION_FIELDS.contains(field)) {
                continue;
            }
            Map<String, Object> writerField = entry.getValue();
            Map<String, Object> readerField = readerProps.get(field);

            if (readerField == null) {
                if (readerIsClosed) {
                    problems.add(field + " is not allowed by the reader schema");
                }
                continue;
            }
            if (!Objects.equals(writerField.get("type"), readerField.get("type"))) {
                problems.add(field + " changed type");
            }
            if (readerField.containsKey("enum")) {
                List<Object> readerValues = (List<Object>) readerField.get("enum");
                List<Object> writerValues = (List<Object>) writerField.get("enum");
                if (writerValues == null || !readerValues.containsAll(writerValues)) {
                    problems.add(field + " accepts fewer values in the reader schema");
                }
            }
        }

        // 2. Every field the reader requires must be guaranteed by the writer
        for (String field : readerRequired) {
            if (!VERSION_FIELDS.contains(field) && !writerRequired.contains(field)) {
                problems.add(field + " is required by the reader but not guaranteed by the writer");
            }
        }
        return problems;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> load(String schemaName) {
        try (InputStream in = getClass().getResourceAsStream("/schemas/" + schemaName + ".json")) {
            return jsonMapper.readValue(in, Map.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read schema " + schemaName, e);
        }
    }
}