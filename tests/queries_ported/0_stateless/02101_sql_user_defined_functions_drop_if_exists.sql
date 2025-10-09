-- Tags: no-parallel

CREATE FUNCTION sql_02101_test_function AS x -> x + 1;

SELECT sql_02101_test_function(1);

DROP FUNCTION sql_02101_test_function;
DROP FUNCTION sql_02101_test_function; --{serverError 46}
DROP FUNCTION IF EXISTS sql_02101_test_function;
