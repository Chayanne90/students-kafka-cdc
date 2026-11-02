package com.cpdevelops.kafka_cdc.students_service.exceptions;

public class EventPublishingException extends RuntimeException {

    public EventPublishingException(String message, Throwable cause) {
        super(message, cause);
    }
}
