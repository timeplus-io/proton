DROP STREAM IF EXISTS test_table;
create stream test_table
(
    id uint64,
    value Date32
) ;

INSERT INTO test_table VALUES (0, toDate32('2019-05-05'));

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary
(
    id uint64,
    value Date32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_table'))
LAYOUT(DIRECT());

SELECT * FROM test_dictionary;
SELECT dictGet('test_dictionary', 'value', to_uint64(0));

DROP DICTIONARY test_dictionary;
DROP STREAM test_table;
