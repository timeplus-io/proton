-- Tags: no-parallel

CREATE FUNCTION sql_02103_test_function AS x -> x + 1;
CREATE FUNCTION sql_02103_test_function_with_nested_function_empty_args AS () -> sql_02103_test_function(1);
CREATE FUNCTION sql_02103_test_function_with_nested_function_arg AS (x) -> sql_02103_test_function(x);

SELECT sql_02103_test_function_with_nested_function_empty_args();
SELECT sql_02103_test_function_with_nested_function_arg(1);

DROP FUNCTION sql_02103_test_function_with_nested_function_empty_args;
DROP FUNCTION sql_02103_test_function_with_nested_function_arg;
DROP FUNCTION sql_02103_test_function;
