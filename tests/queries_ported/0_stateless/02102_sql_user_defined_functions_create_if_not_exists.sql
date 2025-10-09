-- Tags: no-parallel

CREATE FUNCTION IF NOT EXISTS sql_02102_test_function AS x -> x + 1;
SELECT sql_02102_test_function(1);

CREATE FUNCTION sql_02102_test_function AS x -> x + 1; --{serverError 609}
CREATE FUNCTION IF NOT EXISTS sql_02102_test_function AS x -> x + 1;
DROP FUNCTION sql_02102_test_function;
