-- Tags: no-parallel

DROP DATABASE IF EXISTS db_01526;

CREATE DATABASE db_01526;


create stream db_01526.table_for_dict1
(
  key_column uint64,
  second_column uint64,
  third_column string
)
ENGINE = MergeTree()
ORDER BY (key_column, second_column);

INSERT INTO db_01526.table_for_dict1 VALUES (1, 2, 'aaa'), (1, 3, 'bbb'), (2, 3, 'ccc');

CREATE DICTIONARY db_01526.dict1
(
  key_column uint64 DEFAULT 0,
  second_column uint64 DEFAULT 0,
  third_column string DEFAULT 'qqq'
)
PRIMARY KEY key_column, second_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB 'db_01526'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT dictGet('db_01526.dict1', 'third_column', (number, number + 1)) FROM numbers(4);
SELECT dictHas('db_01526.dict1', (to_uint64(1), to_uint64(3)));

DROP DICTIONARY db_01526.dict1;
DROP STREAM db_01526.table_for_dict1;
DROP DATABASE db_01526;
