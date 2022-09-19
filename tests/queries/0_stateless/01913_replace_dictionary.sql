-- Tags: no-parallel

DROP DATABASE IF EXISTS 01913_db;
CREATE DATABASE 01913_db ENGINE=Atomic;

DROP STREAM IF EXISTS 01913_db.test_source_table_1;
create stream 01913_db.test_source_table_1
(
    id uint64,
    value string
) ;

INSERT INTO 01913_db.test_source_table_1 VALUES (0, 'Value0');

DROP DICTIONARY IF EXISTS 01913_db.test_dictionary;
CREATE DICTIONARY 01913_db.test_dictionary
(
    id uint64,
    value string
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB '01913_db' TABLE 'test_source_table_1'));

SELECT * FROM 01913_db.test_dictionary;

DROP STREAM IF EXISTS 01913_db.test_source_table_2;
create stream 01913_db.test_source_table_2
(
    id uint64,
    value_1 string
) ;

INSERT INTO 01913_db.test_source_table_2 VALUES (0, 'Value1');

REPLACE DICTIONARY 01913_db.test_dictionary
(
    id uint64,
    value_1 string
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(DB '01913_db' TABLE 'test_source_table_2'))
LIFETIME(0);

SELECT * FROM 01913_db.test_dictionary;

DROP DICTIONARY 01913_db.test_dictionary;

DROP STREAM 01913_db.test_source_table_1;
DROP STREAM 01913_db.test_source_table_2;

DROP DATABASE 01913_db;
