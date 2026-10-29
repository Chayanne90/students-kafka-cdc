package com.cpdevelops.kafka_cdc.students_service.dto.requests;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateSubmissionRequest(
        @NotBlank @Size(max = 50) String studentID,
        @NotBlank @Size(max = 50) String courseID,
        @NotBlank @Size(max = 50) String assignmentID ) {
}
