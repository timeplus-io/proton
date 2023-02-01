DROP STREAM IF EXISTS test1;
DROP STREAM IF EXISTS test2;

CREATE STREAM test1 (s string) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
CREATE STREAM test2 (s low_cardinality(string)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

INSERT INTO test1 SELECT to_string(number) FROM numbers(10000);
INSERT INTO test2 SELECT to_string(number) FROM numbers(10000);

SELECT s FROM test1 WHERE to_string(s) = '1234' SETTINGS max_rows_to_read = 2;
SELECT s FROM test2 WHERE to_string(s) = '1234' SETTINGS max_rows_to_read = 2;

DROP STREAM test1;
DROP STREAM test2;
