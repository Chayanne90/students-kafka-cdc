package com.cpdevelops.kafka_cdc.students_service.model;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record Submission(
        long id,
        String studentID,
        String courseID,
        String assignmentID,
        BigDecimal score,
        SubmissionStatus status,
        OffsetDateTime submittedAt ) {

    public Boolean conBeGraded() {
        return status != SubmissionStatus.RETURNED;
    }

    public Boolean canBeReturned() {
        return status == SubmissionStatus.GRADED;
    }





}
