-- Tags: long, replica

DROP STREAM IF EXISTS test;
DROP STREAM IF EXISTS test2;

create stream test (x Enum('hello' = 1, 'world' = 2), y string) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01346/table', 'r1') PARTITION BY x ORDER BY y;
create stream test2 (x Enum('hello' = 1, 'world' = 2), y string) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01346/table', 'r2') PARTITION BY x ORDER BY y;
INSERT INTO test VALUES ('hello', 'test');

SELECT * FROM test;
SYSTEM SYNC REPLICA test2;
SELECT * FROM test2;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 3);
INSERT INTO test VALUES ('goodbye', 'test');
OPTIMIZE STREAM test FINAL;
SELECT * FROM test ORDER BY x;
SYSTEM SYNC REPLICA test2;
SELECT * FROM test2 ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2); -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'test' = 3);

ALTER STREAM test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 4); -- { serverError 524 }

ALTER STREAM test MODIFY COLUMN x int8;
INSERT INTO test VALUES (111, 'abc');
OPTIMIZE STREAM test FINAL;
SELECT * FROM test ORDER BY x;
SYSTEM SYNC REPLICA test2;
SELECT * FROM test2 ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test2' AND active ORDER BY partition;

ALTER STREAM test MODIFY COLUMN x Enum8('' = 1); -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x Enum16('' = 1); -- { serverError 524 }

ALTER STREAM test MODIFY COLUMN x uint64; -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x string; -- { serverError 524 }
ALTER STREAM test MODIFY COLUMN x Nullable(Int64); -- { serverError 524 }

ALTER STREAM test RENAME COLUMN x TO z; -- { serverError 524 }
ALTER STREAM test RENAME COLUMN y TO z; -- { serverError 524 }
ALTER STREAM test DROP COLUMN x; -- { serverError 47 }
ALTER STREAM test DROP COLUMN y; -- { serverError 47 }

DROP STREAM test;
DROP STREAM test2;
