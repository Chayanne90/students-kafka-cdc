package com.cpdevelops.kafka_cdc.students_service.dto.requests;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record GradeSubmissionRequest(
        @NotNull
        @DecimalMin("0.0")
        @DecimalMax("100.00")
        @Digits(integer = 3, fraction = 2)
        BigDecimal score ) {
}
