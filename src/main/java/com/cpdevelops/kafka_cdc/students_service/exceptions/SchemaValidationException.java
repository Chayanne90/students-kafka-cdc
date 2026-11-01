package com.cpdevelops.kafka_cdc.students_service.exceptions;

public class SchemaValidationException extends RuntimeException {

    public SchemaValidationException(String message) {
        super(message);
    }
}
