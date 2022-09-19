DROP STREAM IF EXISTS data_01875_1;
DROP STREAM IF EXISTS data_01875_2;
DROP STREAM IF EXISTS data_01875_3;

SET compile_expressions=true;

-- create stream will use global profile with default min_count_to_compile_expression=3
-- so retry 3 times
create stream data_01875_1 Engine=MergeTree ORDER BY number PARTITION BY bitShiftRight(number, 8) + 1 AS SELECT * FROM numbers(16384);
create stream data_01875_2 Engine=MergeTree ORDER BY number PARTITION BY bitShiftRight(number, 8) + 1 AS SELECT * FROM numbers(16384);
create stream data_01875_3 Engine=MergeTree ORDER BY number PARTITION BY bitShiftRight(number, 8) + 1 AS SELECT * FROM numbers(16384);

SELECT number FROM data_01875_3 WHERE number = 999;

DROP STREAM data_01875_1;
DROP STREAM data_01875_2;
DROP STREAM data_01875_3;
