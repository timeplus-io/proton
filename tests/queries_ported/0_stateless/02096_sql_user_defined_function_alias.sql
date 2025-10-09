-- Tags: no-parallel

CREATE FUNCTION sql_02096_test_function AS x -> x + 1;
DESCRIBE (SELECT sql_02096_test_function(1) AS a);
DROP FUNCTION sql_02096_test_function;
