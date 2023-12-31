-- Tags: no-parallel

DROP DATABASE IF EXISTS dict_db_01225;
DROP DATABASE IF EXISTS dict_db_01225_dictionary;

CREATE DATABASE dict_db_01225 ENGINE=Ordinary;    -- Different internal dictionary name with Atomic
CREATE DATABASE dict_db_01225_dictionary Engine=Dictionary;

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

SHOW CREATE STREAM dict_db_01225_dictionary.`dict_db_01225.dict` FORMAT TSVRaw;
SHOW CREATE STREAM dict_db_01225_dictionary.`dict_db_01225.no_such_dict`; -- { serverError 487; }

DROP DATABASE dict_db_01225;
DROP DATABASE dict_db_01225_dictionary;
