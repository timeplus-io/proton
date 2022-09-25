DROP STREAM IF EXISTS dictionary_non_nullable_source_table;
create stream dictionary_non_nullable_source_table (id uint64, value string) ;
INSERT INTO dictionary_non_nullable_source_table VALUES (0, 'Test');

DROP DICTIONARY IF EXISTS test_dictionary_non_nullable;
CREATE DICTIONARY test_dictionary_non_nullable (id uint64, value string) PRIMARY KEY id LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(TABLE 'dictionary_non_nullable_source_table'));

SELECT 'Non nullable value only null key ';
SELECT dictGet('test_dictionary_non_nullable', 'value', NULL);
SELECT 'Non nullable value nullable key';
SELECT dictGet('test_dictionary_non_nullable', 'value', array_join([to_uint64(0), NULL, 1]));

DROP DICTIONARY test_dictionary_non_nullable;
DROP STREAM dictionary_non_nullable_source_table;

DROP STREAM IF EXISTS dictionary_nullable_source_table;
create stream dictionary_nullable_source_table (id uint64, value nullable(string)) ;
INSERT INTO dictionary_nullable_source_table VALUES (0, 'Test'), (1, NULL);

DROP DICTIONARY IF EXISTS test_dictionary_nullable;
CREATE DICTIONARY test_dictionary_nullable (id uint64, value nullable(string)) PRIMARY KEY id LAYOUT(DIRECT()) SOURCE(CLICKHOUSE(TABLE 'dictionary_nullable_source_table'));

SELECT 'nullable value only null key ';
SELECT dictGet('test_dictionary_nullable', 'value', NULL);
SELECT 'nullable value nullable key';
SELECT dictGet('test_dictionary_nullable', 'value', array_join([to_uint64(0), NULL, 1, 2]));

DROP DICTIONARY test_dictionary_nullable;
DROP STREAM dictionary_nullable_source_table;
