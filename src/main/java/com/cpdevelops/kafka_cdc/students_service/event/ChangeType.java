package com.cpdevelops.kafka_cdc.students_service.event;

import com.cpdevelops.kafka_cdc.students_service.exceptions.InvalidSubmissionStateException;

public enum ChangeType {
    CREATED,
    UPDATED,
    DEELTED,
    SNAPSHOT;


    /*op = operation */
    public static ChangeType fromDebezium(String op) {

        return switch (op) {
            case "c" -> CREATED;
            case "u" -> UPDATED;
            case "d" -> DEELTED;
            case "r" -> SNAPSHOT;
            default -> throw new InvalidSubmissionStateException("Unsupported Debezium op: " + op );
        };

    }
}
