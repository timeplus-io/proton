DROP STREAM IF EXISTS test;

CREATE STREAM test (`int8` int8, `int16` int16, `int32` int32, `int64` int64, INDEX idx (`int8`, `int16`, `int32`, `int64`) TYPE bloom_filter(0.01) GRANULARITY 8192 ) ENGINE = MergeTree() ORDER BY `int8`;

INSERT INTO test VALUES (-1, -1, -1, -1);

SELECT * FROM test WHERE `int8` = -1;
SELECT * FROM test WHERE `int16` = -1;
SELECT * FROM test WHERE `int32` = -1;
SELECT * FROM test WHERE `int64` = -1;

DROP STREAM IF EXISTS test;
