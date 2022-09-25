#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    DROP DATABASE IF EXISTS 01685_database_for_cache_dictionary;
    CREATE DATABASE 01685_database_for_cache_dictionary;

    create stream 01685_database_for_cache_dictionary.complex_key_simple_attributes_source_table
    (
    id uint64,
    id_key string,
    value_first string,
    value_second string
    )
    ;

    INSERT INTO 01685_database_for_cache_dictionary.complex_key_simple_attributes_source_table VALUES(0, 'id_key_0', 'value_0', 'value_second_0');
    INSERT INTO 01685_database_for_cache_dictionary.complex_key_simple_attributes_source_table VALUES(1, 'id_key_1', 'value_1', 'value_second_1');
    INSERT INTO 01685_database_for_cache_dictionary.complex_key_simple_attributes_source_table VALUES(2, 'id_key_2', 'value_2', 'value_second_2');

    CREATE DICTIONARY 01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes
    (
    id uint64,
    id_key string,
    value_first string DEFAULT 'value_first_default',
    value_second string DEFAULT 'value_second_default'
    )
    PRIMARY KEY id, id_key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'complex_key_simple_attributes_source_table' DB '01685_database_for_cache_dictionary'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

    SELECT 'Dictionary cache_dictionary_complex_key_simple_attributes';
    SELECT 'dictGet existing value';
    SELECT dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
        dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGet with non existing value';
    SELECT dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
        dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictGetOrDefault existing value';
    SELECT dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
        dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGetOrDefault non existing value';
    SELECT dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
        dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictHas';
    SELECT dictHas('01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes', (number, concat('id_key_', to_string(number)))) FROM system.numbers LIMIT 4;
    SELECT 'select all values as input stream';
    SELECT * FROM 01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes ORDER BY id;

    DROP DICTIONARY 01685_database_for_cache_dictionary.cache_dictionary_complex_key_simple_attributes;
    DROP STREAM 01685_database_for_cache_dictionary.complex_key_simple_attributes_source_table;

    create stream 01685_database_for_cache_dictionary.complex_key_complex_attributes_source_table
    (
    id uint64,
    id_key string,
    value_first string,
    value_second nullable(string)
    )
    ;

    INSERT INTO 01685_database_for_cache_dictionary.complex_key_complex_attributes_source_table VALUES(0, 'id_key_0', 'value_0', 'value_second_0');
    INSERT INTO 01685_database_for_cache_dictionary.complex_key_complex_attributes_source_table VALUES(1, 'id_key_1', 'value_1', NULL);
    INSERT INTO 01685_database_for_cache_dictionary.complex_key_complex_attributes_source_table VALUES(2, 'id_key_2', 'value_2', 'value_second_2');

    CREATE DICTIONARY 01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes
    (
        id uint64,
        id_key string,

        value_first string DEFAULT 'value_first_default',
        value_second nullable(string) DEFAULT 'value_second_default'
    )
    PRIMARY KEY id, id_key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'complex_key_complex_attributes_source_table' DB '01685_database_for_cache_dictionary'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/1d'));

    SELECT 'Dictionary cache_dictionary_complex_key_complex_attributes';
    SELECT 'dictGet existing value';
    SELECT dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
        dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGet with non existing value';
    SELECT dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number)))) as value_first,
        dictGet('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number)))) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictGetOrDefault existing value';
    SELECT dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
        dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 3;
    SELECT 'dictGetOrDefault non existing value';
    SELECT dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_first', (number, concat('id_key_', to_string(number))), to_string('default')) as value_first,
        dictGetOrDefault('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', 'value_second', (number, concat('id_key_', to_string(number))), to_string('default')) as value_second FROM system.numbers LIMIT 4;
    SELECT 'dictHas';
    SELECT dictHas('01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes', (number, concat('id_key_', to_string(number)))) FROM system.numbers LIMIT 4;
    SELECT 'select all values as input stream';
    SELECT * FROM 01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes ORDER BY id;

    DROP DICTIONARY 01685_database_for_cache_dictionary.cache_dictionary_complex_key_complex_attributes;
    DROP STREAM 01685_database_for_cache_dictionary.complex_key_complex_attributes_source_table;

    DROP DATABASE 01685_database_for_cache_dictionary;"
