-- Expand: the new column lives next to the old one
ALTER TABLE submissions ADD COLUMN final_score NUMERIC(5,2);

-- Migrate existing data (backfill)
UPDATE submissions SET final_score = score WHERE score IS NOT NULL AND final_score IS NULL;