package com.cpdevelops.kafka_cdc.students_service.exceptions;

public class SubmissionNotFoundException extends RuntimeException {

    public SubmissionNotFoundException(long id) {
        super("Submission " + id + " not found");
    }
}
