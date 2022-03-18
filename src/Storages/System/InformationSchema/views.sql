ATTACH VIEW views
(
    `table_catalog` string,
    `table_schema` string,
    `table_name` string,
    `view_definition` string,
    `check_option` string,
    `is_updatable` enum8('NO' = 0, 'YES' = 1),
    `is_insertable_into` enum8('NO' = 0, 'YES' = 1),
    `is_trigger_updatable` enum8('NO' = 0, 'YES' = 1),
    `is_trigger_deletable` enum8('NO' = 0, 'YES' = 1),
    `is_trigger_insertable_into` enum8('NO' = 0, 'YES' = 1),
    `TABLE_CATALOG` string ALIAS table_catalog,
    `TABLE_SCHEMA` string ALIAS table_schema,
    `TABLE_NAME` string ALIAS table_name,
    `VIEW_DEFINITION` string ALIAS view_definition,
    `CHECK_OPTION` string ALIAS check_option,
    `IS_UPDATABLE` enum8('NO' = 0, 'YES' = 1) ALIAS is_updatable,
    `IS_INSERTABLE_INTO` enum8('NO' = 0, 'YES' = 1) ALIAS is_insertable_into,
    `IS_TRIGGER_UPDATABLE` enum8('NO' = 0, 'YES' = 1) ALIAS is_trigger_updatable,
    `IS_TRIGGER_DELETABLE` enum8('NO' = 0, 'YES' = 1) ALIAS is_trigger_deletable,
    `IS_TRIGGER_INSERTABLE_INTO` enum8('NO' = 0, 'YES' = 1) ALIAS is_trigger_insertable_into
) AS
SELECT
    database AS table_catalog,
    database AS table_schema,
    name AS table_name,
    as_select AS view_definition,
    'NONE' AS check_option,
    0 AS is_updatable,
    engine = 'MaterializedView' AS is_insertable_into,
    0 AS is_trigger_updatable,
    0 AS is_trigger_deletable,
    0 AS is_trigger_insertable_into
FROM system.tables
WHERE engine LIKE '%View'
