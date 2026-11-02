package com.cpdevelops.kafka_cdc.students_service.exceptions;

public class TransientProcessingException extends RuntimeException{

    public TransientProcessingException(String message) {
        super(message);
    }
}
