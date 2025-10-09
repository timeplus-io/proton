-- Tags: no-parallel

DROP DATABASE IF EXISTS 01681_database_for_cache_dictionary CASCADE;
CREATE DATABASE 01681_database_for_cache_dictionary;

CREATE STREAM 01681_database_for_cache_dictionary.simple_key_simple_attributes_source_table
(
   id uint64,
   value_first string,
   value_second string
)
ENGINE = Memory;

INSERT INTO 01681_database_for_cache_dictionary.simple_key_simple_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO 01681_database_for_cache_dictionary.simple_key_simple_attributes_source_table VALUES(1, 'value_1', 'value_second_1');
INSERT INTO 01681_database_for_cache_dictionary.simple_key_simple_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

CREATE DICTIONARY 01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes
(
   id uint64,
   value_first string DEFAULT 'value_first_default',
   value_second string DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'simple_key_simple_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(CACHE(SIZE_IN_CELLS 10))
SETTINGS(dictionary_use_async_executor=1, max_threads=8)
;

SELECT 'Dictionary cache_dictionary_simple_key_simple_attributes';
SELECT 'dict_get existing value';
SELECT dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dict_get with non existing value';
SELECT dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number) as value_first,
    dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dict_get_or_default existing value';
SELECT dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number, to_string('default')) as value_first,
    dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dict_get_or_default non existing value';
SELECT dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_first', number, to_string('default')) as value_first,
    dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dict_has';
SELECT dict_has('01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes ORDER BY id;

DROP DICTIONARY 01681_database_for_cache_dictionary.cache_dictionary_simple_key_simple_attributes;
DROP STREAM 01681_database_for_cache_dictionary.simple_key_simple_attributes_source_table;

CREATE STREAM 01681_database_for_cache_dictionary.simple_key_complex_attributes_source_table
(
   id uint64,
   value_first string,
   value_second nullable(string)
)
ENGINE = Memory;

INSERT INTO 01681_database_for_cache_dictionary.simple_key_complex_attributes_source_table VALUES(0, 'value_0', 'value_second_0');
INSERT INTO 01681_database_for_cache_dictionary.simple_key_complex_attributes_source_table VALUES(1, 'value_1', NULL);
INSERT INTO 01681_database_for_cache_dictionary.simple_key_complex_attributes_source_table VALUES(2, 'value_2', 'value_second_2');

CREATE DICTIONARY 01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes
(
   id uint64,
   value_first string DEFAULT 'value_first_default',
   value_second nullable(string) DEFAULT 'value_second_default'
)
PRIMARY KEY id
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'simple_key_complex_attributes_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT 'Dictionary cache_dictionary_simple_key_complex_attributes';
SELECT 'dict_get existing value';
SELECT dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 3;
SELECT 'dict_get with non existing value';
SELECT dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number) as value_first,
    dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number) as value_second FROM system.numbers LIMIT 4;
SELECT 'dict_get_or_default existing value';
SELECT dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number, to_string('default')) as value_first,
    dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dict_get_or_default non existing value';
SELECT dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_first', number, to_string('default')) as value_first,
    dict_get_or_default('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', 'value_second', number, to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dict_has';
SELECT dict_has('01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes', number) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes ORDER BY id;

DROP DICTIONARY 01681_database_for_cache_dictionary.cache_dictionary_simple_key_complex_attributes;
DROP STREAM 01681_database_for_cache_dictionary.simple_key_complex_attributes_source_table;

CREATE STREAM 01681_database_for_cache_dictionary.simple_key_hierarchy_table
(
    id uint64,
    parent_id uint64
) ENGINE = Memory();

INSERT INTO 01681_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (1, 0);
INSERT INTO 01681_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (2, 1);
INSERT INTO 01681_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (3, 1);
INSERT INTO 01681_database_for_cache_dictionary.simple_key_hierarchy_table VALUES (4, 2);

CREATE DICTIONARY 01681_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy
(
   id uint64,
   parent_id uint64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'simple_key_hierarchy_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT 'Dictionary cache_dictionary_simple_key_hierarchy';
SELECT 'dict_get';
SELECT dict_get('01681_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy', 'parent_id', number) FROM system.numbers LIMIT 5;
SELECT 'dict_get_hierarchy';
SELECT dict_get_hierarchy('01681_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy', to_uint64(1));
SELECT dict_get_hierarchy('01681_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy', to_uint64(4));

DROP DICTIONARY 01681_database_for_cache_dictionary.cache_dictionary_simple_key_hierarchy;
DROP STREAM 01681_database_for_cache_dictionary.simple_key_hierarchy_table;

DROP DATABASE 01681_database_for_cache_dictionary CASCADE;
