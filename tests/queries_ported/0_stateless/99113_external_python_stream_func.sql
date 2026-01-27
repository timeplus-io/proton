-- DROP FUNCTION IF EXISTS py_table;
--
-- CREATE FUNCTION py_table(val int) RETURNS TABLE(name string, score int32) LANGUAGE PYTHON AS $$
-- def py_table():
--     return [('alice', 5), ('bob', 3)]
-- $$; -- {serverError UNSUPPORTED}
--
-- DROP FUNCTION IF EXISTS py_table;

DROP STREAM IF EXISTS py_table_ext;

CREATE EXTERNAL TABLE py_table_ext(name string, score int32) AS $$
def py_table_ext():
    return [('alice', 5), ('bob', 3)]
$$ SETTINGS type='python'; -- { serverError UNKNOWN_TYPE }

CREATE EXTERNAL STREAM py_table_ext(name string, score int32) AS $$
def py_table_ext():
    return [('alice', 5), ('bob', 3)]
$$ SETTINGS type='python'; 

SHOW CREATE py_table_ext;

SELECT * FROM py_table_ext ORDER BY score;

SELECT name FROM (SELECT * FROM py_table_ext) ORDER BY name;

EXPLAIN SELECT * FROM py_table_ext;

CREATE VIEW py_view AS SELECT * FROM py_table_ext;
SELECT * FROM py_view ORDER BY score;
DROP VIEW py_view;

CREATE EXTERNAL STREAM py_table_no_schema AS $$
def py_table_no_schema():
    return [('alice', 5), ('bob', 3)]
$$ SETTINGS type='python'; -- { clientError SYNTAX_ERROR }

DROP STREAM py_table_ext;
