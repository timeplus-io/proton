-- Tags: no-parallel

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

create stream database_for_dict.table_for_dict (k uint64, v uint8) ENGINE = MergeTree ORDER BY k;

DROP DICTIONARY IF EXISTS database_for_dict.dict1;

CREATE DICTIONARY database_for_dict.dict1 (k uint64 DEFAULT 0, v uint8 DEFAULT 1) PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

DETACH STREAM database_for_dict.dict1; -- { serverError 520 }

DETACH DICTIONARY database_for_dict.dict1;

ATTACH STREAM database_for_dict.dict1; -- { serverError 80 }

ATTACH DICTIONARY database_for_dict.dict1;

DROP DICTIONARY database_for_dict.dict1;

DROP STREAM database_for_dict.table_for_dict;

DROP DATABASE IF EXISTS database_for_dict;
