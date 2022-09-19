SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT 'ComparisionOperator column with same column';

DROP STREAM IF EXISTS test_table;
create stream test_table (a uint64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO test_table VALUES (1);

SELECT test_table.a FROM test_table ORDER BY (test_table.a > test_table.a) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a >= test_table.a) + 1;

SELECT test_table.a FROM test_table ORDER BY (test_table.a < test_table.a) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a <= test_table.a) + 1;

SELECT test_table.a FROM test_table ORDER BY (test_table.a == test_table.a) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a != test_table.a) + 1;

DROP STREAM test_table;

SELECT 'ComparisionOperator column with alias on same column';

DROP STREAM IF EXISTS test_table;
create stream test_table (a uint64, b ALIAS a, c ALIAS b) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO test_table VALUES (1);

SELECT test_table.a FROM test_table ORDER BY (test_table.a > test_table.b) + 1 AND (test_table.a > test_table.c) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a >= test_table.b) + 1 AND (test_table.a >= test_table.c) + 1;

SELECT test_table.a FROM test_table ORDER BY (test_table.a < test_table.b) + 1 AND (test_table.a < test_table.c) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a <= test_table.b) + 1 AND (test_table.a <= test_table.c) + 1;

SELECT test_table.a FROM test_table ORDER BY (test_table.a == test_table.b) + 1 AND (test_table.a == test_table.c) + 1;
SELECT test_table.a FROM test_table ORDER BY (test_table.a != test_table.b) + 1 AND (test_table.a != test_table.c) + 1;

DROP STREAM test_table;
