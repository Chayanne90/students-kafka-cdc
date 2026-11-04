package com.cpdevelops.kafka_cdc.students_service.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.math.RoundingMode;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CourseGradeStats(
        String courseId,
        long gradedCount,
        BigDecimal totalScore) {

    public static CourseGradeStats empty(){
        return new CourseGradeStats(null,0, BigDecimal.ZERO);
    }

    public CourseGradeStats add(String courseId, BigDecimal score) {
        return new CourseGradeStats(courseId, gradedCount+1,totalScore.add(score));
    }

    public CourseGradeStats subtract(String courseId, BigDecimal score) {
        return new CourseGradeStats(courseId, gradedCount - 1, totalScore.subtract(score));
    }

    @JsonProperty("averageScore")
    public BigDecimal avarageScore() {
        if (gradedCount ==0){
            return null;
        }
        return totalScore.divide(BigDecimal.valueOf(gradedCount), 2, RoundingMode.HALF_UP);
    }
}
