ATTACH VIEW tables
(
    `table_catalog` string,
    `table_schema` string,
    `table_name` string,
    `table_type` enum8('BASE TABLE' = 1, 'VIEW' = 2, 'FOREIGN TABLE' = 3, 'LOCAL TEMPORARY' = 4, 'SYSTEM VIEW' = 5),
    `TABLE_CATALOG` string ALIAS table_catalog,
    `TABLE_SCHEMA` string ALIAS table_schema,
    `TABLE_NAME` string ALIAS table_name,
    `TABLE_TYPE` enum8('BASE TABLE' = 1, 'VIEW' = 2, 'FOREIGN TABLE' = 3, 'LOCAL TEMPORARY' = 4, 'SYSTEM VIEW' = 5) ALIAS table_type
) AS
SELECT
    database AS table_catalog,
    database AS table_schema,
    name AS table_name,
    multi_if(is_temporary, 4, engine like '%View', 2, engine LIKE 'System%', 5, has_own_data = 0, 3, 1) AS table_type
FROM system.tables
