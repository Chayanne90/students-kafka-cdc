package com.cpdevelops.kafka_cdc.students_service.exception;

public class SchemaValidationException extends RuntimeException {

    public SchemaValidationException(String message) {
        super(message);
    }
}
