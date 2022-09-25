SET optimize_monotonous_functions_in_order_by = 1;

DROP STREAM IF EXISTS test_order_by;

create stream test_order_by (timestamp datetime, key uint32) ENGINE=MergeTree() ORDER BY (to_date(timestamp), key);
INSERT INTO test_order_by SELECT now() + toIntervalSecond(number), number % 4 FROM numbers(10000);
OPTIMIZE STREAM test_order_by FINAL;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY timestamp LIMIT 10;
EXPLAIN PLAN SELECT * FROM test_order_by ORDER BY timestamp LIMIT 10;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY to_date(timestamp) LIMIT 10;
EXPLAIN PLAN SELECT * FROM test_order_by ORDER BY to_date(timestamp) LIMIT 10;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY to_date(timestamp), timestamp LIMIT 10;
EXPLAIN PLAN SELECT * FROM test_order_by ORDER BY to_date(timestamp), timestamp LIMIT 10;

DROP STREAM IF EXISTS test_order_by;

create stream test_order_by (timestamp datetime, key uint32) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_order_by SELECT now() + toIntervalSecond(number), number % 4 FROM numbers(10000);
OPTIMIZE STREAM test_order_by FINAL;

EXPLAIN SYNTAX SELECT * FROM test_order_by ORDER BY to_date(timestamp), timestamp LIMIT 10;

DROP STREAM IF EXISTS test_order_by;
