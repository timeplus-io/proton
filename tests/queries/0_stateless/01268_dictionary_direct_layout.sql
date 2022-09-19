-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS database_for_dict_01268;

CREATE DATABASE database_for_dict_01268;

DROP STREAM IF EXISTS database_for_dict_01268.table_for_dict1;
DROP STREAM IF EXISTS database_for_dict_01268.table_for_dict2;
DROP STREAM IF EXISTS database_for_dict_01268.table_for_dict3;

create stream database_for_dict_01268.table_for_dict1
(
  key_column uint64,
  second_column uint64,
  third_column string
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict_01268.table_for_dict1 VALUES (100500, 10000000, 'Hello world');

create stream database_for_dict_01268.table_for_dict2
(
  region_id uint64,
  parent_region uint64,
  region_name string
)
ENGINE = MergeTree()
ORDER BY region_id;

INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (1, 0, 'Russia');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (2, 1, 'Moscow');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (3, 2, 'Center');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (4, 0, 'Great Britain');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (5, 4, 'London');

create stream database_for_dict_01268.table_for_dict3
(
  region_id uint64,
  parent_region Float32,
  region_name string
)
ENGINE = MergeTree()
ORDER BY region_id;

INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (1, 0.5, 'Russia');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (2, 1.6, 'Moscow');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (3, 2.3, 'Center');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (4, 0.2, 'Great Britain');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (5, 4.9, 'London');

DROP DATABASE IF EXISTS db_01268;

CREATE DATABASE db_01268;

DROP DICTIONARY IF EXISTS db_01268.dict1;
DROP DICTIONARY IF EXISTS db_01268.dict2;
DROP DICTIONARY IF EXISTS db_01268.dict3;

CREATE DICTIONARY db_01268.dict1
(
  key_column uint64 DEFAULT 0,
  second_column uint64 DEFAULT 1,
  third_column string DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB 'database_for_dict_01268'))
LAYOUT(DIRECT()) SETTINGS(max_result_bytes=1);

CREATE DICTIONARY db_01268.dict2
(
  region_id uint64 DEFAULT 0,
  parent_region uint64 DEFAULT 0 HIERARCHICAL,
  region_name string DEFAULT ''
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict2' PASSWORD '' DB 'database_for_dict_01268'))
LAYOUT(DIRECT());

CREATE DICTIONARY db_01268.dict3
(
  region_id uint64 DEFAULT 0,
  parent_region Float32 DEFAULT 0,
  region_name string DEFAULT ''
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict3' PASSWORD '' DB 'database_for_dict_01268'))
LAYOUT(DIRECT());

SELECT 'INITIALIZING DICTIONARY';

SELECT dictGetHierarchy('db_01268.dict2', to_uint64(3));
SELECT dictHas('db_01268.dict2', to_uint64(3));
SELECT dictHas('db_01268.dict2', to_uint64(45));
SELECT dictIsIn('db_01268.dict2', to_uint64(3), to_uint64(1));
SELECT dictIsIn('db_01268.dict2', to_uint64(1), to_uint64(3));
SELECT dictGetUInt64('db_01268.dict2', 'parent_region', to_uint64(3));
SELECT dictGetUInt64('db_01268.dict2', 'parent_region', to_uint64(99));
SELECT dictGetFloat32('db_01268.dict3', 'parent_region', to_uint64(3));
SELECT dictGetFloat32('db_01268.dict3', 'parent_region', to_uint64(2));
SELECT dictGetFloat32('db_01268.dict3', 'parent_region', to_uint64(1));
SELECT dictGetString('db_01268.dict2', 'region_name', to_uint64(5));
SELECT dictGetString('db_01268.dict2', 'region_name', to_uint64(4));
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(100), 'NONE');

SELECT number + 1, dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(number + 1), 'NONE') chars FROM numbers(10);
SELECT number + 1, dictGetFloat32OrDefault('db_01268.dict3', 'parent_region', to_uint64(number + 1), to_float32(0)) chars FROM numbers(10);
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(1), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(2), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(3), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(4), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(5), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(6), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(7), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(8), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(9), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', to_uint64(10), 'NONE');

SELECT dictGetUInt64('db_01268.dict1', 'second_column', to_uint64(100500)); -- { serverError 396 }

SELECT 'END';

DROP DICTIONARY IF EXISTS db_01268.dict1;
DROP DICTIONARY IF EXISTS db_01268.dict2;
DROP DICTIONARY IF EXISTS db_01268.dict3;

DROP DATABASE IF EXISTS db_01268;

DROP STREAM IF EXISTS database_for_dict_01268.table_for_dict1;
DROP STREAM IF EXISTS database_for_dict_01268.table_for_dict2;
DROP STREAM IF EXISTS database_for_dict_01268.table_for_dict3;

DROP DATABASE IF EXISTS database_for_dict_01268;
