package com.cpdevelops.kafka_cdc.students_service.controller;


import com.cpdevelops.kafka_cdc.students_service.dto.requests.CreateSubmissionRequest;
import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import com.cpdevelops.kafka_cdc.students_service.repository.SubmissionRepository;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

import static org.springframework.web.servlet.function.ServerResponse.notFound;

@RestController
@RequestMapping("api/v1/submissions")
public class SubmissionController {

    private final SubmissionRepository repository;

    public SubmissionController(SubmissionRepository repository) {
        this.repository = repository;
    }

    @PostMapping
    public ResponseEntity<Submission> create (
            @Valid
            @RequestBody
            CreateSubmissionRequest request) {

        Submission created = repository.create(
                request.studentID(), request.courseID(), request.assignmentID());

        URI location = URI.create("/submissions" + created.id());
        return ResponseEntity.created(location).body(created);
    }

    @GetMapping("/{id}")
    public Submission findById(@PathVariable long id) {
        return repository.findById(id).orElseThrow();
    }
}
