DROP STREAM IF EXISTS 02476_query_parameters_insert;
CREATE STREAM 02476_query_parameters_insert (x int32) ENGINE=MergeTree() ORDER BY tuple();

SET param_x = 1;
INSERT INTO 02476_query_parameters_insert VALUES ({x: int32});
SELECT * FROM 02476_query_parameters_insert;

DROP STREAM 02476_query_parameters_insert;
