DROP STREAM IF EXISTS test;
create stream test (x int8, y int8, z int8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (1, 3, 3), (1, 4, 3), (2, 5, 4), (2, 2, 4);

SET optimize_monotonous_functions_in_order_by = 1;
SELECT * FROM test ORDER BY to_float32(x), -y, -z DESC;
SELECT * FROM test ORDER BY to_float32(x), -(-y), -z DESC;
SELECT max(x) as k FROM test ORDER BY k;
SELECT round_to_exp2(x) as k FROM test GROUP BY k ORDER BY k;
SELECT round_to_exp2(x) as k, y, z FROM test WHERE k >= 1 ORDER BY k;
SELECT max(x) as k FROM test HAVING k > 0 ORDER BY k;

SET optimize_monotonous_functions_in_order_by = 0;
SELECT * FROM test ORDER BY to_float32(x), -y, -z DESC;
SELECT * FROM test ORDER BY to_float32(x), -(-y), -z DESC;
SELECT max(x) as k FROM test ORDER BY k;
SELECT round_to_exp2(x) as k From test GROUP BY k ORDER BY k;
SELECT round_to_exp2(x) as k, y, z FROM test WHERE k >= 1 ORDER BY k;
SELECT max(x) as k FROM test HAVING k > 0 ORDER BY k;

DROP STREAM test;
