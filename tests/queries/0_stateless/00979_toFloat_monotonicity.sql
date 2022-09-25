DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;
DROP STREAM IF EXISTS test3;

create stream test1 (n uint64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 1;
create stream test2 (s string) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
create stream test3 (d Decimal(4, 3)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;

INSERT INTO test1 SELECT * FROM numbers(10000);
SELECT n FROM test1 WHERE to_float64(n) = 7777.0 SETTINGS max_rows_to_read = 2;
SELECT n FROM test1 WHERE to_float32(n) = 7777.0 SETTINGS max_rows_to_read = 2;

INSERT INTO test2 SELECT to_string(number) FROM numbers(10000);
SELECT s FROM test2 WHERE to_float64(s) = 7777.0;
SELECT s FROM test2 WHERE to_float32(s) = 7777.0;

INSERT INTO test3 SELECT to_decimal64(number, 3) FROM numbers(10000);
SELECT d FROM test3 WHERE to_float64(d) = 7777.0 SETTINGS max_rows_to_read = 2;
SELECT d FROM test3 WHERE to_float32(d) = 7777.0 SETTINGS max_rows_to_read = 2;

DROP STREAM test1;
DROP STREAM test2;
DROP STREAM test3;
