-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS 01837_db;
CREATE DATABASE 01837_db ;

DROP STREAM IF EXISTS 01837_db.simple_key_dictionary_source;
create stream 01837_db.simple_key_dictionary_source
(
    id uint64,
    value string
) ;

INSERT INTO 01837_db.simple_key_dictionary_source VALUES (1, 'First');
INSERT INTO 01837_db.simple_key_dictionary_source VALUES (2, 'Second');
INSERT INTO 01837_db.simple_key_dictionary_source VALUES (3, 'Third');

DROP DICTIONARY IF EXISTS 01837_db.simple_key_direct_dictionary;
CREATE DICTIONARY 01837_db.simple_key_direct_dictionary
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB '01837_db' TABLE 'simple_key_dictionary_source'))
LAYOUT(DIRECT());

SELECT * FROM 01837_db.simple_key_direct_dictionary;

DROP DICTIONARY 01837_db.simple_key_direct_dictionary;
DROP STREAM 01837_db.simple_key_dictionary_source;

DROP DATABASE 01837_db;
