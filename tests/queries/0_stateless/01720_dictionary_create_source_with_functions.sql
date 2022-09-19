-- Tags: no-parallel

DROP DATABASE IF EXISTS 01720_dictionary_db;
CREATE DATABASE 01720_dictionary_db;

create stream 01720_dictionary_db.dictionary_source_table
(
	key uint8,
    value string
)
;

INSERT INTO 01720_dictionary_db.dictionary_source_table VALUES (1, 'First');

CREATE DICTIONARY 01720_dictionary_db.dictionary
(
    key uint64,
    value string
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(DB '01720_dictionary_db' TABLE 'dictionary_source_table' HOST hostName() PORT tcpPort()))
LIFETIME(0)
LAYOUT(FLAT());

SELECT * FROM 01720_dictionary_db.dictionary;

DROP DICTIONARY 01720_dictionary_db.dictionary;
DROP STREAM 01720_dictionary_db.dictionary_source_table;

DROP DATABASE 01720_dictionary_db;
