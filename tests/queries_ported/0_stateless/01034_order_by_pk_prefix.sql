DROP STREAM IF EXISTS test_table;

CREATE STREAM test_table (n int32, s string)
ENGINE = MergeTree() PARTITION BY n % 10 ORDER BY n;

INSERT INTO test_table SELECT number, to_string(number) FROM system.numbers LIMIT 100;
INSERT INTO test_table SELECT number, to_string(number * number) FROM system.numbers LIMIT 100;
INSERT INTO test_table SELECT number, to_string(number * number) FROM system.numbers LIMIT 100;

SELECT * FROM test_table ORDER BY n, s LIMIT 30;

DROP STREAM test_table;
