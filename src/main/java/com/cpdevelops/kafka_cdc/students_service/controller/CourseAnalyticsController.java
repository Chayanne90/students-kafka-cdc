package com.cpdevelops.kafka_cdc.students_service.controller;


import com.cpdevelops.kafka_cdc.students_service.service.CourseAnalyticsQueryService;
import com.cpdevelops.kafka_cdc.students_service.streams.model.CourseActivity;
import com.cpdevelops.kafka_cdc.students_service.streams.model.CourseGradeStats;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.List;

@RestController
@RequestMapping("api/v1/analytics")
public class CourseAnalyticsController {

    private final CourseAnalyticsQueryService queryService;

    public CourseAnalyticsController(CourseAnalyticsQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/courses/grade-stats")
    public List<CourseGradeStats> allGradeStats() {
        return queryService.allGradeStats();
    }

    @GetMapping("/courses/{courseId}/grade-stats")
    public CourseGradeStats gradeStats(@PathVariable String courseId) {
        return queryService.gradeStats(courseId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "No graded submissions for course " + courseId));
    }

    @GetMapping("/courses/{courseId}/activity")
    public List<CourseActivity> activity(@PathVariable String courseId,
                                         @RequestParam(defaultValue = "15") int minutes) {
        return queryService.recentActivity(courseId, Duration.ofMinutes(minutes));
    }

    @GetMapping("/topology")
    public String topology() {
        return queryService.describeTopology();
    }
}
