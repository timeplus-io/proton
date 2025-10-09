CREATE STREAM set_crash (key1 int32, id1 int64, c1 int64) PARTITION BY id1 ORDER BY key1;
SELECT sleep(2);
INSERT INTO set_crash(key1, id1, c1) VALUES (-1, 1, 0);
SELECT sleep(2);
SELECT 1 in (-1, 1) FROM table(set_crash) WHERE (key1, id1) in (-1, 1);