ATTACH VIEW schemata
(
    `catalog_name` string,
    `schema_name` string,
    `schema_owner` string,
    `default_character_set_catalog` nullable(string),
    `default_character_set_schema` nullable(string),
    `default_character_set_name` nullable(string),
    `sql_path` nullable(string),
    `CATALOG_NAME` string ALIAS catalog_name,
    `SCHEMA_NAME` string ALIAS schema_name,
    `SCHEMA_OWNER` string ALIAS schema_owner,
    `DEFAULT_CHARACTER_SET_CATALOG` nullable(string) ALIAS default_character_set_catalog,
    `DEFAULT_CHARACTER_SET_SCHEMA` nullable(string) ALIAS default_character_set_schema,
    `DEFAULT_CHARACTER_SET_NAME` nullable(string) ALIAS default_character_set_name,
    `SQL_PATH` nullable(string) ALIAS sql_path
) AS
SELECT
    name AS catalog_name,
    name AS schema_name,
    'default' AS schema_owner,
    NULL AS default_character_set_catalog,
    NULL AS default_character_set_schema,
    NULL AS default_character_set_name,
    NULL AS sql_path
FROM system.databases
