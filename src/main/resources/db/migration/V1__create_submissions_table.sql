CREATE TABLE submissions (
    id             BIGSERIAL     PRIMARY KEY,
    student_id     VARCHAR(50)   NOT NULL,
    course_id      VARCHAR(50)   NOT NULL,
    assignment_id  VARCHAR(50)   NOT NULL,
    score          NUMERIC(5,2),
    status         VARCHAR(20)   NOT NULL DEFAULT 'SUBMITTED',
    submitted_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_submissions_course ON submissions (course_id);

-- On UPDATE/DELETE, write the full old row to the WAL,
-- so Debezium events include a complete "before" image.
ALTER TABLE submissions REPLICA IDENTITY FULL;
