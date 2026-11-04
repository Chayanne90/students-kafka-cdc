-- How many minutes after the deadline the submission was made (NULL = on time / unknown)
ALTER TABLE submissions ADD COLUMN late_minutes INTEGER;

ALTER TABLE submissions
    ADD CONSTRAINT chk_late_minutes_non_negative
    CHECK (late_minutes IS NULL OR late_minutes >= 0);