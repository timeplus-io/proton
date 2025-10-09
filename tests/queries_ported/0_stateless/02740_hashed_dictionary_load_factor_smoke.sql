DROP STREAM IF EXISTS test_table;
CREATE STREAM test_table
(
    key uint64,
    value uint16
) ENGINE=Memory() AS SELECT number, number FROM numbers(1e5);

DROP STREAM IF EXISTS test_table_nullable;
CREATE STREAM test_table_nullable
(
    key uint64,
    value nullable(uint16)
) ENGINE=Memory() AS SELECT number, number % 2 == 0 ? NULL : number FROM numbers(1e5);

DROP STREAM IF EXISTS test_table_string;
CREATE STREAM test_table_string
(
    key string,
    value uint16
) ENGINE=Memory() AS SELECT 'foo' || number::string, number FROM numbers(1e5);

DROP STREAM IF EXISTS test_table_complex;
CREATE STREAM test_table_complex
(
    key_1 uint64,
    key_2 uint64,
    value uint16
) ENGINE=Memory() AS SELECT number, number, number FROM numbers(1e5);

DROP DICTIONARY IF EXISTS test_sparse_dictionary_load_factor;
CREATE DICTIONARY test_sparse_dictionary_load_factor
(
    key uint64,
    value uint16
) PRIMARY KEY key
SOURCE(TIMEPLUS(STREAM test_table USER proton PASSWORD 'proton@t+'))
LAYOUT(SPARSE_HASHED(MAX_LOAD_FACTOR 0.90))
LIFETIME(0);
SHOW CREATE test_sparse_dictionary_load_factor;
SYSTEM RELOAD DICTIONARY test_sparse_dictionary_load_factor;
SELECT element_count FROM system.dictionaries WHERE database = current_database() AND name = 'test_sparse_dictionary_load_factor';
SELECT count() FROM test_table WHERE dict_get('test_sparse_dictionary_load_factor', 'value', key) != value;
DROP DICTIONARY test_sparse_dictionary_load_factor;

DROP DICTIONARY IF EXISTS test_dictionary_load_factor;
CREATE DICTIONARY test_dictionary_load_factor
(
    key uint64,
    value uint16
) PRIMARY KEY key
SOURCE(TIMEPLUS(STREAM test_table USER proton PASSWORD 'proton@t+'))
LAYOUT(HASHED(MAX_LOAD_FACTOR 0.90))
LIFETIME(0);
SHOW CREATE test_dictionary_load_factor;
SYSTEM RELOAD DICTIONARY test_dictionary_load_factor;
SELECT element_count FROM system.dictionaries WHERE database = current_database() AND name = 'test_dictionary_load_factor';
SELECT count() FROM test_table WHERE dict_get('test_dictionary_load_factor', 'value', key) != value;
DROP DICTIONARY test_dictionary_load_factor;

DROP DICTIONARY IF EXISTS test_dictionary_load_factor_nullable;
CREATE DICTIONARY test_dictionary_load_factor_nullable
(
    key uint64,
    value nullable(uint16)
) PRIMARY KEY key
SOURCE(TIMEPLUS(STREAM test_table_nullable USER proton PASSWORD 'proton@t+'))
LAYOUT(HASHED(MAX_LOAD_FACTOR 0.90))
LIFETIME(0);
SHOW CREATE test_dictionary_load_factor_nullable;
SYSTEM RELOAD DICTIONARY test_dictionary_load_factor_nullable;
SELECT element_count FROM system.dictionaries WHERE database = current_database() AND name = 'test_dictionary_load_factor_nullable';
SELECT count() FROM test_table_nullable WHERE dict_get('test_dictionary_load_factor_nullable', 'value', key) != value;
DROP DICTIONARY test_dictionary_load_factor_nullable;

DROP DICTIONARY IF EXISTS test_complex_dictionary_load_factor;
CREATE DICTIONARY test_complex_dictionary_load_factor
(
    key_1 uint64,
    key_2 uint64,
    value uint16
) PRIMARY KEY key_1, key_2
SOURCE(TIMEPLUS(STREAM test_table_complex USER proton PASSWORD 'proton@t+'))
LAYOUT(COMPLEX_KEY_HASHED(MAX_LOAD_FACTOR 0.90))
LIFETIME(0);
SYSTEM RELOAD DICTIONARY test_complex_dictionary_load_factor;
SHOW CREATE test_complex_dictionary_load_factor;
SELECT element_count FROM system.dictionaries WHERE database = current_database() and name = 'test_complex_dictionary_load_factor';
SELECT count() FROM test_table_complex WHERE dict_get('test_complex_dictionary_load_factor', 'value', (key_1, key_2)) != value;
DROP DICTIONARY test_complex_dictionary_load_factor;

DROP DICTIONARY IF EXISTS test_dictionary_load_factor_string;
CREATE DICTIONARY test_dictionary_load_factor_string
(
    key string,
    value uint16
) PRIMARY KEY key
SOURCE(TIMEPLUS(STREAM test_table_string USER proton PASSWORD 'proton@t+'))
LAYOUT(HASHED(MAX_LOAD_FACTOR 1))
LIFETIME(0);
-- should because of MAX_LOAD_FACTOR is 1 (maximum allowed value is 0.99)
SYSTEM RELOAD DICTIONARY test_dictionary_load_factor_string; -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY test_dictionary_load_factor_string;

DROP STREAM test_table;
DROP STREAM test_table_nullable;
DROP STREAM test_table_string;
DROP STREAM test_table_complex;
