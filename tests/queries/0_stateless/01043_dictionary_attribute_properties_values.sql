-- Tags: no-parallel

DROP DATABASE IF EXISTS dictdb_01043;
CREATE DATABASE dictdb_01043;

create stream dictdb_01043.dicttbl(key int64, value_default string, value_expression string) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dictdb_01043.dicttbl VALUES (12, 'hello', '55:66:77');


CREATE DICTIONARY dictdb_01043.dict
(
  key int64 DEFAULT -1,
  value_default string DEFAULT 'world',
  value_expression string DEFAULT 'xxx' EXPRESSION 'to_string(127 * 172)'

)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dicttbl' DB 'dictdb_01043'))
LAYOUT(FLAT())
LIFETIME(1);


SELECT dictGetString('dictdb_01043.dict', 'value_default', to_uint64(12));
SELECT dictGetString('dictdb_01043.dict', 'value_default', to_uint64(14));

SELECT dictGetString('dictdb_01043.dict', 'value_expression', to_uint64(12));
SELECT dictGetString('dictdb_01043.dict', 'value_expression', to_uint64(14));

DROP DATABASE IF EXISTS dictdb_01043;
