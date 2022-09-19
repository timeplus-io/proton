-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

DROP STREAM IF EXISTS database_for_dict.table_for_dict;

create stream database_for_dict.table_for_dict
(
  key_column uint64,
  second_column uint64,
  third_column string
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict.table_for_dict VALUES (100500, 10000000, 'Hello world');

DROP DATABASE IF EXISTS ordinary_db;

CREATE DATABASE ordinary_db;

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

CREATE DICTIONARY ordinary_db.dict1
(
  key_column uint64 DEFAULT 0,
  second_column uint64 DEFAULT 1,
  third_column string DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()) SETTINGS(max_result_bytes=1);

SELECT 'INITIALIZING DICTIONARY';

SELECT dictGetUInt64('ordinary_db.dict1', 'second_column', to_uint64(100500)); -- { serverError 396 }

SELECT 'END';

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

DROP DATABASE IF EXISTS ordinary_db;

DROP STREAM IF EXISTS database_for_dict.table_for_dict;

DROP DATABASE IF EXISTS database_for_dict;
