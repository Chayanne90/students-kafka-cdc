package com.cpdevelops.kafka_cdc.students_service.exceptions;

public class InvalidSubmissionStateException extends RuntimeException {

    public InvalidSubmissionStateException(String message) {
        super(message);
    }
}
