-- Tags: no-parallel

DROP DATABASE IF EXISTS dict_db_01225;
CREATE DATABASE dict_db_01225;

CREATE STREAM dict_db_01225.dict_data (key uint64, val uint64) Engine=Memory();
CREATE DICTIONARY dict_db_01225.dict
(
  key uint64 DEFAULT 0,
  val uint64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'dict_data' PASSWORD '' DB 'dict_db_01225'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SYSTEM RELOAD DICTIONARY dict_db_01225.dict;

DROP STREAM dict_db_01225.dict; -- { serverError 520; }
DROP DICTIONARY dict_db_01225.dict;

DROP DATABASE dict_db_01225;
