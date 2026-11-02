package com.cpdevelops.kafka_cdc.students_service.event;

import com.cpdevelops.kafka_cdc.students_service.exceptions.InvalidChangeEventException;
import tools.jackson.databind.JsonNode;

public record DebeziumChangeEvent(
        String op,
        JsonNode before,
        JsonNode after,
        long lsn,
        long sourceTimestamps) {


    public static DebeziumChangeEvent from (JsonNode root) {
        if (!root.hasNonNull("op") || !root.hasNonNull("source")) {
            throw new InvalidChangeEventException("Not a Debesium change event: missing [op] or [source]");
        }

        JsonNode source = root.get("source");
        return new DebeziumChangeEvent(
                root.get("op").asString(),
                root.get("before"),
                root.get("after"),
                source.path("lsn").asLong(0L),
                source.path("ts_ms").asLong(0L));
    }


    public JsonNode rowState() {
        JsonNode row = "d".equals(op) ? before : after;
        if (row == null || row.isNull()) {
            throw new InvalidChangeEventException("Change event with op"  + op +" has no row data");
        }
        return row;
    }
}
