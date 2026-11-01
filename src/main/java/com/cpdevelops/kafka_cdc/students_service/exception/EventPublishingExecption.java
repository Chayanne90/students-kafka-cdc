package com.cpdevelops.kafka_cdc.students_service.exception;

public class EventPublishingExecption extends RuntimeException {

    public EventPublishingExecption(String message, Throwable cause) {
        super(message, cause);
    }
}
