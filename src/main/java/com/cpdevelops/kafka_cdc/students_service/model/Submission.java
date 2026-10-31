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


   /* if not returned it can be graded*/
    public Boolean canBeGraded() {
        return status != SubmissionStatus.RETURNED;
    }

    /* Only graded work can be returned */
    public Boolean canBeReturned() {
        return status == SubmissionStatus.GRADED;
    }
}
