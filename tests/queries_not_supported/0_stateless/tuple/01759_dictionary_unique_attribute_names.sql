-- Tags: no-parallel

DROP DATABASE IF EXISTS 01759_db;
CREATE DATABASE 01759_db;

create stream 01759_db.dictionary_source_table
(
   key uint64,
   value1 uint64,
   value2 uint64
)
;

INSERT INTO 01759_db.dictionary_source_table VALUES (0, 2, 3), (1, 5, 6), (2, 8, 9);

CREATE DICTIONARY 01759_db.test_dictionary(key uint64, value1 uint64, value1 uint64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table' DB '01759_db'))
LAYOUT(COMPLEX_KEY_DIRECT()); -- {serverError 36}

CREATE DICTIONARY 01759_db.test_dictionary(key uint64, value1 uint64, value2 uint64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table' DB '01759_db'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT number, dictGet('01759_db.test_dictionary', 'value1', tuple(number)) as value1,
   dictGet('01759_db.test_dictionary', 'value2', tuple(number)) as value2 FROM system.numbers LIMIT 3;

DROP DATABASE 01759_db;
