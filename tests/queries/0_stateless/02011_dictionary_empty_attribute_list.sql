-- Tags: no-parallel

DROP STREAM IF EXISTS test_table;
create stream test_table (id uint64) ;
INSERT INTO test_table VALUES (0);

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary (id uint64) PRIMARY KEY id LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(TABLE 'test_table'));
SELECT * FROM test_dictionary;
SELECT dictHas('test_dictionary', to_uint64(0));
SELECT dictHas('test_dictionary', to_uint64(1));

DROP DICTIONARY test_dictionary;
DROP STREAM test_table;
