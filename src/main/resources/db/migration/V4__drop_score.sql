-- Contract: nothing reads or writes the old column anymore
ALTER TABLE submissions DROP COLUMN score;