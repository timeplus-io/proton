DROP STREAM IF EXISTS 02176_test_simple_key_table;
create stream 02176_test_simple_key_table
(
    id uint64,
    value string
) ;

INSERT INTO 02176_test_simple_key_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02176_test_simple_key_dictionary;
CREATE DICTIONARY 02176_test_simple_key_dictionary
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02176_test_simple_key_table'))
LAYOUT(DIRECT());

SELECT dictGet('02176_test_simple_key_dictionary', 'value', to_uint64(0));
SELECT dictGet('02176_test_simple_key_dictionary', 'value', to_uint8(0));
SELECT dictGet('02176_test_simple_key_dictionary', 'value', '0');
SELECT dictGet('02176_test_simple_key_dictionary', 'value', [0]); --{serverError 43}

SELECT dictHas('02176_test_simple_key_dictionary', to_uint64(0));
SELECT dictHas('02176_test_simple_key_dictionary', to_uint8(0));
SELECT dictHas('02176_test_simple_key_dictionary', '0');
SELECT dictHas('02176_test_simple_key_dictionary', [0]); --{serverError 43}

DROP DICTIONARY 02176_test_simple_key_dictionary;
DROP STREAM 02176_test_simple_key_table;

DROP STREAM IF EXISTS 02176_test_complex_key_table;
create stream 02176_test_complex_key_table
(
    id uint64,
    id_key string,
    value string
) ;

INSERT INTO 02176_test_complex_key_table VALUES (0, '0', 'Value');

DROP DICTIONARY IF EXISTS 02176_test_complex_key_dictionary;
CREATE DICTIONARY 02176_test_complex_key_dictionary
(
    id uint64,
    id_key string,
    value string
)
PRIMARY KEY id, id_key
SOURCE(CLICKHOUSE(TABLE '02176_test_complex_key_table'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT dictGet('02176_test_complex_key_dictionary', 'value', tuple(to_uint64(0), '0'));
SELECT dictGet('02176_test_complex_key_dictionary', 'value', tuple(to_uint8(0), '0'));
SELECT dictGet('02176_test_complex_key_dictionary', 'value', tuple('0', '0'));
SELECT dictGet('02176_test_complex_key_dictionary', 'value', tuple([0], '0')); --{serverError 43}
SELECT dictGet('02176_test_complex_key_dictionary', 'value', tuple(to_uint64(0), 0));

SELECT dictHas('02176_test_complex_key_dictionary', tuple(to_uint64(0), '0'));
SELECT dictHas('02176_test_complex_key_dictionary', tuple(to_uint8(0), '0'));
SELECT dictHas('02176_test_complex_key_dictionary', tuple('0', '0'));
SELECT dictHas('02176_test_complex_key_dictionary', tuple([0], '0')); --{serverError 43}
SELECT dictHas('02176_test_complex_key_dictionary', tuple(to_uint64(0), 0));

DROP DICTIONARY 02176_test_complex_key_dictionary;
DROP STREAM 02176_test_complex_key_table;
