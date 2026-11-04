package com.cpdevelops.kafka_cdc.students_service.exceptions;

/* Kafka Streams state can't be required right now (starting, rebalancing, restoring)*/
public class AnalyticsUnavailableException extends  RuntimeException {

    public AnalyticsUnavailableException(String message) {
        super(message);
    }

    public AnalyticsUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
