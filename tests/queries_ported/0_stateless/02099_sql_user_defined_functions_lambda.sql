-- Tags: no-parallel
CREATE FUNCTION sql_02099_lambda_function AS x -> array_map(array_element -> array_element * 2, x);
SELECT sql_02099_lambda_function([1,2,3]);
DROP FUNCTION sql_02099_lambda_function;
