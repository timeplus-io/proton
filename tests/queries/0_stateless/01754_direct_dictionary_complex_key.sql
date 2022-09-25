-- Tags: no-parallel

DROP DATABASE IF EXISTS 01754_dictionary_db;
CREATE DATABASE 01754_dictionary_db;

create stream 01754_dictionary_db.complex_key_simple_attributes_source_table
(
   id uint64,
   id_key string,
   value_first string,
   value_second string
)
;

INSERT INTO 01754_dictionary_db.complex_key_simple_attributes_source_table VALUES(0, 'id_key_0', 'value_0', 'value_second_0');
INSERT INTO 01754_dictionary_db.complex_key_simple_attributes_source_table VALUES(1, 'id_key_1', 'value_1', 'value_second_1');
INSERT INTO 01754_dictionary_db.complex_key_simple_attributes_source_table VALUES(2, 'id_key_2', 'value_2', 'value_second_2');

CREATE DICTIONARY 01754_dictionary_db.direct_dictionary_complex_key_simple_attributes
(
   id uint64,
   id_key string DEFAULT 'test_default_id_key',
   value_first string DEFAULT 'value_first_default',
   value_second string DEFAULT 'value_second_default'
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'complex_key_simple_attributes_source_table'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Dictionary direct_dictionary_complex_key_simple_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
    dictGet('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
    dictGet('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
    dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
    dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('01754_dictionary_db.direct_dictionary_complex_key_simple_attributes', (number, concat('id_key_', to_string(number)))) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01754_dictionary_db.direct_dictionary_complex_key_simple_attributes;

DROP DICTIONARY 01754_dictionary_db.direct_dictionary_complex_key_simple_attributes;
DROP STREAM 01754_dictionary_db.complex_key_simple_attributes_source_table;

create stream 01754_dictionary_db.complex_key_complex_attributes_source_table
(
   id uint64,
   id_key string,
   value_first string,
   value_second nullable(string)
)
;

INSERT INTO 01754_dictionary_db.complex_key_complex_attributes_source_table VALUES(0, 'id_key_0', 'value_0', 'value_second_0');
INSERT INTO 01754_dictionary_db.complex_key_complex_attributes_source_table VALUES(1, 'id_key_1', 'value_1', NULL);
INSERT INTO 01754_dictionary_db.complex_key_complex_attributes_source_table VALUES(2, 'id_key_2', 'value_2', 'value_second_2');

CREATE DICTIONARY 01754_dictionary_db.direct_dictionary_complex_key_complex_attributes
(
    id uint64,
    id_key string,

    value_first string DEFAULT 'value_first_default',
    value_second nullable(string) DEFAULT 'value_second_default'
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'complex_key_complex_attributes_source_table'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Dictionary direct_dictionary_complex_key_complex_attributes';
SELECT 'dictGet existing value';
SELECT dictGet('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
    dictGet('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGet with non existing value';
SELECT dictGet('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
    dictGet('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictGetOrDefault existing value';
SELECT dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
    dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 3;
SELECT 'dictGetOrDefault non existing value';
SELECT dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
    dictGetOrDefault('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 4;
SELECT 'dictHas';
SELECT dictHas('01754_dictionary_db.direct_dictionary_complex_key_complex_attributes', (number, concat('id_key_', to_string(number)))) FROM system.numbers LIMIT 4;
SELECT 'select all values as input stream';
SELECT * FROM 01754_dictionary_db.direct_dictionary_complex_key_complex_attributes;

DROP DICTIONARY 01754_dictionary_db.direct_dictionary_complex_key_complex_attributes;
DROP STREAM 01754_dictionary_db.complex_key_complex_attributes_source_table;

DROP DATABASE 01754_dictionary_db;
