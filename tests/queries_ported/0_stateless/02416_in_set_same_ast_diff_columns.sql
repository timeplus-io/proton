CREATE STREAM set_crash (key1 int32, id1 int64, c1 int64) ENGINE = MergeTree PARTITION BY id1 ORDER BY key1;
INSERT INTO set_crash VALUES (-1, 1, 0);
SELECT 1 in (-1, 1) FROM set_crash WHERE (key1, id1) in (-1, 1);
