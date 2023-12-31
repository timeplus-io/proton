DROP STREAM IF EXISTS test;

CREATE STREAM test (key uint64, a uint8, b string, c float64) ENGINE = MergeTree() ORDER BY key;
INSERT INTO test SELECT number, number, to_string(number), number from numbers(4);

set optimize_redundant_functions_in_order_by = 1;

SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) as s FULL JOIN test as t USING(key) ORDER BY s.key, t.key;
SELECT key, a FROM test ORDER BY key, a, exp(key + a);
SELECT key, a FROM test ORDER BY key, exp(key + a);
EXPLAIN SYNTAX SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
EXPLAIN SYNTAX SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
EXPLAIN SYNTAX SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
EXPLAIN SYNTAX SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) as s FULL JOIN test as t USING(key) ORDER BY s.key, t.key;
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, a, exp(key + a);
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, exp(key + a);

set optimize_redundant_functions_in_order_by = 0;

SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) as s FULL JOIN test as t USING(key) ORDER BY s.key, t.key;
SELECT key, a FROM test ORDER BY key, a, exp(key + a);
SELECT key, a FROM test ORDER BY key, exp(key + a);
EXPLAIN SYNTAX SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
EXPLAIN SYNTAX SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(exp(x)));
EXPLAIN SYNTAX SELECT group_array(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
EXPLAIN SYNTAX SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) as s FULL JOIN test as t USING(key) ORDER BY s.key, t.key;
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, a, exp(key + a);
EXPLAIN SYNTAX SELECT key, a FROM test ORDER BY key, exp(key + a);

DROP STREAM test;
