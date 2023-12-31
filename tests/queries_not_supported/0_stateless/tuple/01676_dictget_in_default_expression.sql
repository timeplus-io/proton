-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01676 SYNC;

CREATE DATABASE test_01676;

create stream test_01676.dict_data (key uint64, value uint64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test_01676.dict_data VALUES (2,20), (3,30), (4,40), (5,50);

CREATE DICTIONARY test_01676.dict (key uint64, value uint64) PRIMARY KEY key SOURCE(CLICKHOUSE(DB 'test_01676' TABLE 'dict_data' HOST '127.0.0.1' PORT tcpPort())) LIFETIME(0) LAYOUT(HASHED());

create stream test_01676.table (x uint64, y uint64 DEFAULT dictGet('test_01676.dict', 'value', x)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test_01676.table (x) VALUES (2);
INSERT INTO test_01676.table VALUES (to_uint64(3), to_uint64(15));

SELECT * FROM test_01676.table ORDER BY x;

SELECT 'status:';
SELECT status FROM system.dictionaries WHERE database='test_01676' AND name='dict';

DETACH DATABASE test_01676;
ATTACH DATABASE test_01676;

SELECT 'status_after_detach_and_attach:';
SELECT status FROM system.dictionaries WHERE database='test_01676' AND name='dict';

INSERT INTO test_01676.table (x) VALUES (to_int64(4));
SELECT * FROM test_01676.table ORDER BY x;

SELECT 'status:';
SELECT status FROM system.dictionaries WHERE database='test_01676' AND name='dict';

DROP DATABASE test_01676;
