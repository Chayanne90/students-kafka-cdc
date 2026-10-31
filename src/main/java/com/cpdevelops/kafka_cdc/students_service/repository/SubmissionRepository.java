package com.cpdevelops.kafka_cdc.students_service.repository;

import com.cpdevelops.kafka_cdc.students_service.model.Submission;
import com.cpdevelops.kafka_cdc.students_service.model.SubmissionStatus;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public class SubmissionRepository {

    private static final String columns =
            "id, student_id, course_id, assignment_id, score, status, submitted_at";

    private static final RowMapper<Submission> row_mapper = (rs, rowNum) -> new Submission(
            rs.getLong("id"),
            rs.getString("student_id"),
            rs.getString("course_id"),
            rs.getString("assignment_id"),
            rs.getBigDecimal("score"),
            SubmissionStatus.valueOf(rs.getString("status")),
            rs.getObject("submitted_at", OffsetDateTime.class));

    private final JdbcClient jdbc;

    public SubmissionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Submission insert(String studentId, String courseId, String assignmentId) {
        return jdbc.sql("""
                        INSERT INTO submissions (student_id, course_id, assignment_id)
                        VALUES (:studentId, :courseId, :assignmentId)
                        RETURNING\s""" + columns)
                .param("studentId", studentId)
                .param("courseId", courseId)
                .param("assignmentId", assignmentId)
                .query(row_mapper)
                .single();
    }

    public Optional<Submission> findById(long id) {
        return jdbc.sql("SELECT " + columns + " FROM submissions WHERE id = :id")
                .param("id", id)
                .query(row_mapper)
                .optional();
    }

    public Optional<Submission> findByIdForUpdate(long id) {
        return jdbc.sql("SELECT " + columns + " FROM submissions WHERE id = :id FOR UPDATE")
                .param("id", id)
                .query(row_mapper)
                .optional();
    }

    public List<Submission> findByCourseId(String courseId) {
        return jdbc.sql("SELECT " + columns + " FROM submissions WHERE course_id = :courseId ORDER BY id")
                .param("courseId", courseId)
                .query(row_mapper)
                .list();
    }

    public Submission updateScoreAndStatus(long id, BigDecimal score, SubmissionStatus status) {
        return jdbc.sql("""
                        UPDATE submissions SET score = :score, status = :status
                        WHERE id = :id
                        RETURNING\s""" + columns)
                .param("score",score)
                .param("status", status.name())
                .param("id", id)
                .query(row_mapper)
                .single();
    }

    public Submission updateStatus(long id, SubmissionStatus status) {
        return jdbc.sql("""
                UPDATE submissions SET status = :status
                WHERE ID = :id
                RETURNING\s""" + columns)
                .param("status", status.name())
                .param("id",id)
                .query(row_mapper)
                .single();
    }

    public boolean deleteById(long id) {
        return jdbc.sql("DELETE FROM submissions WHERE id = :id")
                .param("id",id)
                .update() > 0;
    }
}
