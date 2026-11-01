package com.cpdevelops.kafka_cdc.students_service.exception;

public class InvalidChangeEventException extends RuntimeException{

    public InvalidChangeEventException(String message) {
        super(message);
    }

    public InvalidChangeEventException(String message, Throwable cause) {
        super(message,cause);
    }

}
