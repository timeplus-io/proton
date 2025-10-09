-- Tags: no-parallel
CREATE FUNCTION sql_02098_alias_function AS x -> (((x * 2) AS x_doubled) + x_doubled);
SELECT sql_02098_alias_function(2);
DROP FUNCTION sql_02098_alias_function;
