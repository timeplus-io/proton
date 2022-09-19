-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01915_db;
CREATE DATABASE test_01915_db ENGINE=Atomic;

DROP STREAM IF EXISTS test_01915_db.test_source_table_1;
create stream test_01915_db.test_source_table_1
(
    id uint64,
    value string
) ;

INSERT INTO test_01915_db.test_source_table_1 VALUES (0, 'Value0');

DROP DICTIONARY IF EXISTS test_01915_db.test_dictionary;
CREATE OR REPLACE DICTIONARY test_01915_db.test_dictionary
(
    id uint64,
    value string
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB 'test_01915_db' TABLE 'test_source_table_1'));

SELECT * FROM test_01915_db.test_dictionary;

DROP STREAM IF EXISTS test_01915_db.test_source_table_2;
create stream test_01915_db.test_source_table_2
(
    id uint64,
    value_1 string
) ;

INSERT INTO test_01915_db.test_source_table_2 VALUES (0, 'Value1');

CREATE OR REPLACE DICTIONARY test_01915_db.test_dictionary
(
    id uint64,
    value_1 string
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(DB 'test_01915_db' TABLE 'test_source_table_2'))
LIFETIME(0);

SELECT * FROM test_01915_db.test_dictionary;

DROP DICTIONARY test_01915_db.test_dictionary;

DROP STREAM test_01915_db.test_source_table_1;
DROP STREAM test_01915_db.test_source_table_2;

DROP DATABASE test_01915_db;
