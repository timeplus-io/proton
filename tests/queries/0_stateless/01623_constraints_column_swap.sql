SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_substitute_columns = 1;
SET optimize_append_index = 1;

DROP STREAM IF EXISTS column_swap_test_test;

create stream column_swap_test_test (i int64, a string, b uint64, CONSTRAINT c1 ASSUME b = cityHash64(a)) ENGINE = MergeTree() ORDER BY i;
INSERT INTO column_swap_test_test VALUES (1, 'cat', 1), (2, 'dog', 2);
INSERT INTO column_swap_test_test SELECT number AS i, format('test {} kek {}', to_string(number), to_string(number + 10))  AS a, 1 AS b FROM system.numbers LIMIT 1000000;

EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE cityHash64(a) = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE cityHash64(a) = 0;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE b = 0;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE b = 1;

EXPLAIN SYNTAX SELECT cityHash64(a) + 10 FROM column_swap_test_test WHERE cityHash64(a) = 0;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, a FROM column_swap_test_test WHERE cityHash64(a) = 0;
EXPLAIN SYNTAX SELECT b + 10, a FROM column_swap_test_test WHERE b = 0;

DROP STREAM column_swap_test_test;

create stream column_swap_test_test (i int64, a string, b string, CONSTRAINT c1 ASSUME a = substring(reverse(b), 1, 1)) ENGINE = MergeTree() ORDER BY i;
INSERT INTO column_swap_test_test SELECT number AS i, to_string(number) AS a, format('test {} kek {}', to_string(number), to_string(number + 10)) b FROM system.numbers LIMIT 1000000;

EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1), a FROM column_swap_test_test WHERE a = 'c';
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1), a FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c';
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1) AS t1, a AS t2 FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c';
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1) FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c';

DROP STREAM column_swap_test_test;

DROP STREAM IF EXISTS t_bad_constraint;

create stream t_bad_constraint(a uint32, s string, CONSTRAINT c1 ASSUME a = to_uint32(s)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bad_constraint SELECT number, randomPrintableASCII(100) FROM numbers(10000);

EXPLAIN SYNTAX SELECT a FROM t_bad_constraint;

DROP STREAM t_bad_constraint;
