-- Tags: no-parallel

DROP STREAM IF EXISTS table_function_dictionary_source_table;
create stream table_function_dictionary_source_table
(
   id uint64,
   value uint64
)
;

INSERT INTO table_function_dictionary_source_table VALUES (0, 0);
INSERT INTO table_function_dictionary_source_table VALUES (1, 1);

DROP DICTIONARY IF EXISTS table_function_dictionary_test_dictionary;
CREATE DICTIONARY table_function_dictionary_test_dictionary
(
   id uint64,
   value uint64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_function_dictionary_source_table'))
LAYOUT(DIRECT());

SELECT * FROM dictionary('table_function_dictionary_test_dictionary');

DROP STREAM table_function_dictionary_source_table;
DROP DICTIONARY table_function_dictionary_test_dictionary;
