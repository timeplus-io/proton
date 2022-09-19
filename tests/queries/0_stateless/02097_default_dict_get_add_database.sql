-- Tags: no-parallel

DROP DATABASE IF EXISTS 02097_db;
CREATE DATABASE 02097_db;

USE 02097_db;

create stream test_table
(
    key_column uint64,
    data_column_1 uint64,
    data_column_2 uint8
)
ENGINE = MergeTree
ORDER BY key_column;

CREATE DICTIONARY test_dictionary
(
    key_column uint64 DEFAULT 0,
    data_column_1 uint64 DEFAULT 1,
    data_column_2 uint8 DEFAULT 1
)
PRIMARY KEY key_column
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'test_table'));

create stream test_table_default
(
    data_1 DEFAULT dictGetUInt64('test_dictionary', 'data_column_1', to_uint64(0)),
    data_2 DEFAULT dictGet(test_dictionary, 'data_column_2', to_uint64(0))
)
;

SELECT create_table_query FROM system.tables WHERE name = 'test_table_default' AND database = '02097_db';

DROP STREAM test_table_default;
DROP DICTIONARY test_dictionary;
DROP STREAM test_table;

DROP DATABASE IF EXISTS 02097_db;
