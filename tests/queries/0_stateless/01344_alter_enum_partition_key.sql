DROP STREAM IF EXISTS test;
create stream test (x Enum('hello' = 1, 'world' = 2), y string) ENGINE = MergeTree PARTITION BY x ORDER BY y;
INSERT INTO test VALUES ('hello', 'test');

SELECT * FROM test;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;

ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 3);
INSERT INTO test VALUES ('goodbye', 'test');
OPTIMIZE STREAM test FINAL;
SELECT * FROM test ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;

ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2); -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'test' = 3);
ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 4); -- { serverError 524 }

ALTER STREAM test MODIFY COLUMN x int8;
INSERT INTO test VALUES (111, 'abc');
OPTIMIZE STREAM test FINAL;
SELECT * FROM test ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;

ALTER STREAM test MODIFY COLUMN x Enum8('' = 1); -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x Enum16('' = 1); -- { serverError 524 }

ALTER STREAM test MODIFY COLUMN x uint64; -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x string; -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x Nullable(int64); -- { serverError 524 }

ALTER STREAM test RENAME COLUMN x TO z; -- { serverError 524 }
ALTER STREAM test RENAME COLUMN y TO z; -- { serverError 524 }
ALTER STREAM test DROP COLUMN x; -- { serverError 47 }
ALTER STREAM test DROP COLUMN y; -- { serverError 47 }

DROP STREAM test;
