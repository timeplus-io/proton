-- Tags: no-parallel, no-backward-compatibility-check

DROP DATABASE IF EXISTS test_01191;
CREATE DATABASE test_01191 ENGINE=Atomic;

CREATE STREAM test_01191._ (n uint64, s string) ENGINE = Memory();
CREATE STREAM test_01191.t (n uint64, s string) ENGINE = Memory();

CREATE DICTIONARY test_01191.dict (n uint64, s string)
PRIMARY KEY n
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM '_' DB 'test_01191'));

INSERT INTO test_01191._ VALUES (42, 'test');

SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;

RENAME DICTIONARY test_01191.table TO test_01191.table1; -- {serverError 60}
EXCHANGE DICTIONARIES test_01191._ AND test_01191.dict; -- {serverError 80}
EXCHANGE STREAMS test_01191.t AND test_01191.dict;
SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;
SELECT dictGet(test_01191.t, 's', to_uint64(42));
EXCHANGE STREAMS test_01191.dict AND test_01191.t;
RENAME DICTIONARY test_01191.t TO test_01191.dict1; -- {serverError 80}
DROP DICTIONARY test_01191.t; -- {serverError 80}
DROP STREAM test_01191.t;

CREATE DATABASE dummy_db ENGINE=Atomic;
RENAME DICTIONARY test_01191.dict TO dummy_db.dict1;
RENAME DICTIONARY dummy_db.dict1 TO test_01191.dict;
DROP DATABASE dummy_db;

RENAME DICTIONARY test_01191.dict TO test_01191.dict1;

SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;
SELECT dictGet(test_01191.dict1, 's', to_uint64(42));

RENAME DICTIONARY test_01191.dict1 TO test_01191.dict2;

SELECT name, status FROM system.dictionaries WHERE database='test_01191';
SELECT name, engine FROM system.tables WHERE database='test_01191' ORDER BY name;
SELECT dictGet(test_01191.dict2, 's', to_uint64(42));

DROP DATABASE test_01191;
