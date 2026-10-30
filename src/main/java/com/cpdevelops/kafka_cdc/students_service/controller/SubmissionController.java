package com.cpdevelops.kafka_cdc.students_service.controller;


import com.cpdevelops.kafka_cdc.students_service.dto.requests.CreateSubmissionRequest;
import com.cpdevelops.kafka_cdc.students_service.dto.requests.GradeSubmissionRequest;
import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import com.cpdevelops.kafka_cdc.students_service.repository.SubmissionRepository;
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
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("api/v1/submissions")
public class SubmissionController {

    private final SubmissionRepository repository;

    public SubmissionController(SubmissionRepository repository) {
        this.repository = repository;
    }

    @PostMapping
    public ResponseEntity<Submission> create(@Valid @RequestBody CreateSubmissionRequest request) {

        Submission created = repository.create(request.studentId(), request.courseId(), request.assignmentId());
        URI location = URI.create("api/v1/submissions" + created.id());
        return ResponseEntity.created(location).body(created);
    }

    @GetMapping("/{id}")
    public Submission findById(@PathVariable long id) {
        return repository.findById(id).orElseThrow(() -> notFound(id));
    }

    @GetMapping
    public List<Submission> findByCourse(@RequestParam String courseID) {
        return repository.findByCourse(courseID);
    }

    @PutMapping("/{id}/grade")
    public Submission grade(@PathVariable long id, @Valid @RequestBody GradeSubmissionRequest request) {
        return repository.grade(id, request.score()).orElseThrow(() -> notFound(id));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable long id) {
        if (!repository.delete(id)) throw notFound(id);
    }

    private static ResponseStatusException notFound( long id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Submission" + id + " not found");
    }
}
