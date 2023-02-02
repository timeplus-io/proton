-- Tags: no-parallel

DROP DATABASE IF EXISTS dict_db_01254;
CREATE DATABASE dict_db_01254;

CREATE STREAM dict_db_01254.dict_data (key uint64, val uint64) Engine=Memory();
CREATE DICTIONARY dict_db_01254.dict
(
  key uint64 DEFAULT 0,
  val uint64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'dict_data' PASSWORD '' DB 'dict_db_01254'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

DETACH DATABASE dict_db_01254;
ATTACH DATABASE dict_db_01254;

SELECT query_count, status FROM system.dictionaries WHERE database = 'dict_db_01254' AND name = 'dict';
SYSTEM RELOAD DICTIONARY dict_db_01254.dict;
SELECT query_count, status FROM system.dictionaries WHERE database = 'dict_db_01254' AND name = 'dict';
SELECT dictGetuint64('dict_db_01254.dict', 'val', to_uint64(0));
SELECT query_count, status FROM system.dictionaries WHERE database = 'dict_db_01254' AND name = 'dict';

DROP DATABASE dict_db_01254;
