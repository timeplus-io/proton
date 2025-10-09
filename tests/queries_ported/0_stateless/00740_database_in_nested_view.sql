-- Tags: no-parallel
set joined_subquery_requires_alias=0;
DROP DATABASE IF EXISTS test_00740 CASCADE;
CREATE DATABASE test_00740;
USE test_00740;

DROP STREAM IF EXISTS test_view_00740;
DROP STREAM IF EXISTS test_nested_view_00740;
DROP STREAM IF EXISTS test_joined_view_00740;
DROP STREAM IF EXISTS test_00740;

CREATE VIEW test_00740 AS SELECT 1 AS N;
CREATE VIEW test_view_00740 AS SELECT * FROM table(test_00740);
CREATE VIEW test_nested_view_00740 AS SELECT * FROM (SELECT * FROM table(test_00740));
CREATE VIEW test_joined_view_00740 AS SELECT *, N AS x FROM table(test_00740) ANY LEFT JOIN table(test_00740) USING N SETTINGS joined_subquery_requires_alias=0;

SELECT * FROM test_view_00740;
SELECT * FROM test_nested_view_00740;
SELECT * FROM test_joined_view_00740;

USE default;
SELECT * FROM test_00740.test_view_00740;
SELECT * FROM test_00740.test_nested_view_00740;
SELECT * FROM test_00740.test_joined_view_00740;

DROP STREAM IF EXISTS test_00740.test_view_00740;
DROP STREAM IF EXISTS test_00740.test_nested_view_00740;
DROP STREAM IF EXISTS test_00740.test_joined_view_00740;
DROP STREAM IF EXISTS test_00740.test_00740;

DROP DATABASE test_00740;