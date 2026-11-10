DROP STREAM IF EXISTS test;
CREATE STREAM test (col1 nullable(float64), col2 nullable(float64), col3 float64) ENGINE = Memory;

INSERT INTO test VALUES (1.0, 2.0, 3.0);
SELECT multi_if(col1 > 2, col2 / col3, 4.0) FROM test SETTINGS min_count_to_compile_expression = 0;

DROP STREAM test;

-- A non-nullable branch cast to a nullable result, feeding another compiled function
SELECT if(idx = 1, 0.0, n) * f FROM (SELECT to_nullable(to_float64(number)) AS n, to_float64(number) AS f, 1 + number % 3 AS idx FROM numbers(6)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT multi_if(idx = 1, 0.0, idx = 2, n, 1.0) * f FROM (SELECT to_nullable(to_float64(number)) AS n, to_float64(number) AS f, 1 + number % 3 AS idx FROM numbers(6)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT if(idx = 1, f + 1, n) * f FROM (SELECT to_nullable(to_float64(number)) AS n, to_float64(number) AS f, 1 + number % 3 AS idx FROM numbers(6)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
