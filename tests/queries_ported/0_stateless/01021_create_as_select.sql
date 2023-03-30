DROP STREAM IF EXISTS create_as_select_01021;
CREATE STREAM create_as_select_01021 engine=Memory AS (SELECT (1, 1));
SELECT * FROM create_as_select_01021;
DROP STREAM create_as_select_01021;
