package com.cpdevelops.kafka_cdc.students_service.controller;


import com.cpdevelops.kafka_cdc.students_service.dto.requests.CreateSubmissionRequest;
import com.cpdevelops.kafka_cdc.students_service.dto.requests.GradeSubmissionRequest;
import com.cpdevelops.kafka_cdc.students_service.dto.responses.SubmissionResponse;
import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import com.cpdevelops.kafka_cdc.students_service.service.SubmissionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("api/v1/submissions")
public class SubmissionController {

    private final SubmissionService service;

    public SubmissionController(SubmissionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<SubmissionResponse> create(@Valid @RequestBody CreateSubmissionRequest request) {

        Submission created = service.submit(request.studentId(), request.courseId(), request.assignmentId());
        URI location = URI.create("api/v1/submissions" + created.id());
        return ResponseEntity.created(location).body(SubmissionResponse.from(created));
    }

    @GetMapping("/{id}")
    public SubmissionResponse getById(@PathVariable long id) {
        return SubmissionResponse.from(service.getById(id));
    }

    @GetMapping
    public List<SubmissionResponse> getByCourse(@RequestParam String courseId) {
        return service.getByCourse(courseId).stream()
                .map(SubmissionResponse::from)
                .toList();
    }

    @PutMapping("/{id}/grade")
    public SubmissionResponse grade(@PathVariable long id, @Valid @RequestBody GradeSubmissionRequest request) {
        return SubmissionResponse.from(service.grade(id, request.score()));
    }

    @PutMapping("/{id}/return")
    public SubmissionResponse returnToStudent(@PathVariable long id) {
        return SubmissionResponse.from(service.returnToStudent(id));
    }
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable long id) {
        service.delete(id);
    }

}
