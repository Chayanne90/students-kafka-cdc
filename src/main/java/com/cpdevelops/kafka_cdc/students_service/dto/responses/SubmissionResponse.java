package com.cpdevelops.kafka_cdc.students_service.dto.responses;

import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import com.cpdevelops.kafka_cdc.students_service.model.SubmissionStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record SubmissionResponse(long id,
                                 String studentID,
                                 String courseID,
                                 String assignmentID,
                                 BigDecimal score,
                                 SubmissionStatus status,
                                 OffsetDateTime submittedAt ) {

    public static SubmissionResponse from(Submission submission) {
        return new SubmissionResponse(
        submission.id(),
        submission.studentID(),
        submission.assignmentID(),
        submission.courseID(),
        submission.score(),
        submission.status(),
        submission.submittedAt());

    }
}
