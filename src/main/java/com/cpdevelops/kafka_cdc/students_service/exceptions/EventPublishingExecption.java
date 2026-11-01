package com.cpdevelops.kafka_cdc.students_service.exceptions;

public class EventPublishingExecption extends RuntimeException {

    public EventPublishingExecption(String message, Throwable cause) {
        super(message, cause);
    }
}
