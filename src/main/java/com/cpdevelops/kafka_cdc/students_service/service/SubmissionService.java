package com.cpdevelops.kafka_cdc.students_service.service;

import com.cpdevelops.kafka_cdc.students_service.exceptions.InvalidSubmissionStateException;
import com.cpdevelops.kafka_cdc.students_service.exceptions.SubmissionNotFoundException;
import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import com.cpdevelops.kafka_cdc.students_service.model.SubmissionStatus;
import com.cpdevelops.kafka_cdc.students_service.repository.SubmissionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Service
public class SubmissionService {

    private final SubmissionRepository repository;


    public SubmissionService(SubmissionRepository repository){
        this.repository = repository;
    }

    @Transactional
    public Submission submit(String studentId, String courseId, String assignmentId) {
        return repository.insert(studentId,courseId,assignmentId);
    }

    @Transactional(readOnly = true)
    public Submission getById(long id) {
        return repository.findById(id)
                .orElseThrow(() -> new SubmissionNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public List<Submission> getByCourse(String courseId){
        return repository.findByCourseId(courseId);
    }

    @Transactional
    public Submission grade(long id, BigDecimal score){
        Submission current = repository.findByIdForUpdate(id)
                .orElseThrow(() -> new SubmissionNotFoundException(id));

        if(!current.canBeGraded()) {
            throw new InvalidSubmissionStateException(
                    "Submission " + id + " is " + current.status() + " and no longer be graded"
            );
        }
        return repository.updateScoreAndStatus(id, score, SubmissionStatus.GRADED);
    }


    @Transactional
    public Submission returnToStudent(long id) {
        Submission current = repository.findByIdForUpdate(id)
                .orElseThrow(() -> new SubmissionNotFoundException(id));
        if (!current.canBeReturned()) {
            throw new InvalidSubmissionStateException(
                    "Submission " + id + " is " + current.status() + "; only GRADED submissions can be returned"
            );

        }
        return repository.updateStatus(id, SubmissionStatus.RETURNED);
    }

    @Transactional
    public void delete(long id) {
        if(!repository.deleteById(id)) throw new SubmissionNotFoundException(id);
    }

}
