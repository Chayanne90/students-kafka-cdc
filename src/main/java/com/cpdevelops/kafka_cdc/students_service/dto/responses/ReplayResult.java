package com.cpdevelops.kafka_cdc.students_service.dto.responses;

public record ReplayResult(int replayed, int skipped) {
}
