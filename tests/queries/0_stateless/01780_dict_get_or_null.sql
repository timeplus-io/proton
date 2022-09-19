DROP STREAM IF EXISTS simple_key_dictionary_source_table;
create stream simple_key_dictionary_source_table
(
    id uint64,
    value string,
    value_nullable Nullable(string)
) ;

INSERT INTO simple_key_dictionary_source_table VALUES (1, 'First', 'First');
INSERT INTO simple_key_dictionary_source_table VALUES (2, 'Second', NULL);
INSERT INTO simple_key_dictionary_source_table VALUES (3, 'Third', 'Third');

DROP DICTIONARY IF EXISTS simple_key_dictionary;
CREATE DICTIONARY simple_key_dictionary
(
    id uint64,
    value string,
    value_nullable Nullable(string)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_dictionary_source_table'))
LAYOUT(DIRECT());

SELECT 'Simple key dictionary dictGetOrNull';

SELECT
    number,
    dictHas('simple_key_dictionary', number),
    dictGetOrNull('simple_key_dictionary', 'value', number),
    dictGetOrNull('simple_key_dictionary', 'value_nullable', number),
    dictGetOrNull('simple_key_dictionary', ('value', 'value_nullable'), number)
FROM system.numbers LIMIT 5;

DROP DICTIONARY simple_key_dictionary;
DROP STREAM simple_key_dictionary_source_table;

DROP STREAM IF EXISTS complex_key_dictionary_source_table;
create stream complex_key_dictionary_source_table
(
    id uint64,
    id_key string,
    value string,
    value_nullable Nullable(string)
) ;

INSERT INTO complex_key_dictionary_source_table VALUES (1, 'key', 'First', 'First');
INSERT INTO complex_key_dictionary_source_table VALUES (2, 'key', 'Second', NULL);
INSERT INTO complex_key_dictionary_source_table VALUES (3, 'key', 'Third', 'Third');

DROP DICTIONARY IF EXISTS complex_key_dictionary;
CREATE DICTIONARY complex_key_dictionary
(
    id uint64,
    id_key string,
    value string,
    value_nullable Nullable(string)
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'complex_key_dictionary_source_table'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT 'Complex key dictionary dictGetOrNull';

SELECT
    (number, 'key'),
    dictHas('complex_key_dictionary', (number, 'key')),
    dictGetOrNull('complex_key_dictionary', 'value', (number, 'key')),
    dictGetOrNull('complex_key_dictionary', 'value_nullable', (number, 'key')),
    dictGetOrNull('complex_key_dictionary', ('value', 'value_nullable'), (number, 'key'))
FROM system.numbers LIMIT 5;

DROP DICTIONARY complex_key_dictionary;
DROP STREAM complex_key_dictionary_source_table;

DROP STREAM IF EXISTS range_key_dictionary_source_table;
create stream range_key_dictionary_source_table
(
    key uint64,
    start_date date,
    end_date date,
    value string,
    value_nullable Nullable(string)
)
();

INSERT INTO range_key_dictionary_source_table VALUES(1, to_date('2019-05-20'), to_date('2019-05-20'), 'First', 'First');
INSERT INTO range_key_dictionary_source_table VALUES(2, to_date('2019-05-20'), to_date('2019-05-20'), 'Second', NULL);
INSERT INTO range_key_dictionary_source_table VALUES(3, to_date('2019-05-20'), to_date('2019-05-20'), 'Third', 'Third');

DROP DICTIONARY IF EXISTS range_key_dictionary;
CREATE DICTIONARY range_key_dictionary
(
    key uint64,
    start_date date,
    end_date date,
    value string,
    value_nullable Nullable(string)
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);

SELECT 'Range key dictionary dictGetOrNull';

SELECT
    (number, to_date('2019-05-20')),
    dictHas('range_key_dictionary', number, to_date('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value', number, to_date('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value_nullable', number, to_date('2019-05-20')),
    dictGetOrNull('range_key_dictionary', ('value', 'value_nullable'), number, to_date('2019-05-20'))
FROM system.numbers LIMIT 5;

DROP DICTIONARY range_key_dictionary;
DROP STREAM range_key_dictionary_source_table;
