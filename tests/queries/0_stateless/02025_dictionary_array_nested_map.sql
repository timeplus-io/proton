create stream dict_nested_map_test_table
(
	test_id uint32,
	type string,
	test_config array(Map(string, Decimal(28,12))),
	ncp uint8
)
ENGINE=MergeTree()
ORDER BY test_id;

INSERT INTO dict_nested_map_test_table VALUES (3, 't', [{'l': 0.0, 'h': 10000.0, 't': 0.1}, {'l': 10001.0, 'h': 100000000000000.0, 't': 0.2}], 0);

CREATE DICTIONARY dict_nested_map_dictionary
(
	test_id uint32,
	type string,
	test_config array(Map(string, Decimal(28,12))),
	ncp uint8
)
PRIMARY KEY test_id
SOURCE(CLICKHOUSE(TABLE 'dict_nested_map_test_table'))
LAYOUT(HASHED(PREALLOCATE 1))
LIFETIME(MIN 1 MAX 1000000);

SELECT dictGet('dict_nested_map_dictionary', 'test_config', to_uint64(3));
