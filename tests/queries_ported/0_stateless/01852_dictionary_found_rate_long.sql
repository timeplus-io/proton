-- Tags: long, no-parallel

--
-- Simple key
--

DROP STREAM IF EXISTS simple_key_source_table_01862;
CREATE STREAM simple_key_source_table_01862
(
    id uint64,
    value string
) ENGINE = Memory();

INSERT INTO simple_key_source_table_01862 VALUES (1, 'First');
INSERT INTO simple_key_source_table_01862 VALUES (1, 'First');

-- simple flat
DROP DICTIONARY IF EXISTS simple_key_flat_dictionary_01862;
CREATE DICTIONARY simple_key_flat_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'simple_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT * FROM simple_key_flat_dictionary_01862 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_flat_dictionary_01862';
SELECT * FROM simple_key_flat_dictionary_01862 WHERE id = 0 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_flat_dictionary_01862';
SELECT dict_get('simple_key_flat_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, round(found_rate, 2) FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_flat_dictionary_01862';

DROP DICTIONARY simple_key_flat_dictionary_01862;

-- simple direct
DROP DICTIONARY IF EXISTS simple_key_direct_dictionary_01862;
CREATE DICTIONARY simple_key_direct_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'simple_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(DIRECT());

-- check that found_rate is 0, not nan
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_direct_dictionary_01862';
SELECT * FROM simple_key_direct_dictionary_01862 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_direct_dictionary_01862';
SELECT dict_get('simple_key_direct_dictionary_01862', 'value', to_uint64(1)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_direct_dictionary_01862';
SELECT dict_get('simple_key_direct_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_direct_dictionary_01862';

DROP DICTIONARY simple_key_direct_dictionary_01862;

-- simple hashed
DROP DICTIONARY IF EXISTS simple_key_hashed_dictionary_01862;
CREATE DICTIONARY simple_key_hashed_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'simple_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_hashed_dictionary_01862';
SELECT dict_get('simple_key_hashed_dictionary_01862', 'value', to_uint64(1)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_hashed_dictionary_01862';
SELECT dict_get('simple_key_hashed_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_hashed_dictionary_01862';

DROP DICTIONARY simple_key_hashed_dictionary_01862;

-- simple sparse_hashed
DROP DICTIONARY IF EXISTS simple_key_sparse_hashed_dictionary_01862;
CREATE DICTIONARY simple_key_sparse_hashed_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'simple_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(SPARSE_HASHED())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_sparse_hashed_dictionary_01862';
SELECT dict_get('simple_key_sparse_hashed_dictionary_01862', 'value', to_uint64(1)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_sparse_hashed_dictionary_01862';
SELECT dict_get('simple_key_sparse_hashed_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_sparse_hashed_dictionary_01862';

DROP DICTIONARY simple_key_sparse_hashed_dictionary_01862;

-- simple cache
DROP DICTIONARY IF EXISTS simple_key_cache_dictionary_01862;
CREATE DICTIONARY simple_key_cache_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'simple_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(CACHE(SIZE_IN_CELLS 100000))
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_cache_dictionary_01862';
SELECT to_uint64(1) as key, dict_get('simple_key_cache_dictionary_01862', 'value', key) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_cache_dictionary_01862';
SELECT to_uint64(2) as key, dict_get('simple_key_cache_dictionary_01862', 'value', key) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_cache_dictionary_01862';

DROP DICTIONARY simple_key_cache_dictionary_01862;

DROP STREAM simple_key_source_table_01862;

--
-- Complex key
--

DROP STREAM IF EXISTS complex_key_source_table_01862;
CREATE STREAM complex_key_source_table_01862
(
    id uint64,
    id_key string,
    value string
) ENGINE = Memory();

INSERT INTO complex_key_source_table_01862 VALUES (1, 'FirstKey', 'First');
INSERT INTO complex_key_source_table_01862 VALUES (1, 'FirstKey', 'First');

-- complex hashed
DROP DICTIONARY IF EXISTS complex_key_hashed_dictionary_01862;
CREATE DICTIONARY complex_key_hashed_dictionary_01862
(
    id uint64,
    id_key string,
    value string
)
PRIMARY KEY id, id_key
SOURCE(TIMEPLUS(STREAM 'complex_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_hashed_dictionary_01862';
SELECT dict_get('complex_key_hashed_dictionary_01862', 'value', (to_uint64(1), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_hashed_dictionary_01862';
SELECT dict_get('complex_key_hashed_dictionary_01862', 'value', (to_uint64(2), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_hashed_dictionary_01862';

DROP DICTIONARY complex_key_hashed_dictionary_01862;

-- complex direct
DROP DICTIONARY IF EXISTS complex_key_direct_dictionary_01862;
CREATE DICTIONARY complex_key_direct_dictionary_01862
(
    id uint64,
    id_key string,
    value string
)
PRIMARY KEY id, id_key
SOURCE(TIMEPLUS(STREAM 'complex_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_direct_dictionary_01862';
SELECT dict_get('complex_key_direct_dictionary_01862', 'value', (to_uint64(1), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_direct_dictionary_01862';
SELECT dict_get('complex_key_direct_dictionary_01862', 'value', (to_uint64(2), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_direct_dictionary_01862';

DROP DICTIONARY complex_key_direct_dictionary_01862;

-- complex cache
DROP DICTIONARY IF EXISTS complex_key_cache_dictionary_01862;
CREATE DICTIONARY complex_key_cache_dictionary_01862
(
    id uint64,
    id_key string,
    value string
)
PRIMARY KEY id, id_key
SOURCE(TIMEPLUS(STREAM 'complex_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 100000))
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_cache_dictionary_01862';
SELECT dict_get('complex_key_cache_dictionary_01862', 'value', (to_uint64(1), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_cache_dictionary_01862';
SELECT dict_get('complex_key_cache_dictionary_01862', 'value', (to_uint64(2), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'complex_key_cache_dictionary_01862';

DROP DICTIONARY complex_key_cache_dictionary_01862;

DROP STREAM complex_key_source_table_01862;

--
-- Range
--
DROP STREAM IF EXISTS range_key_source_table_01862;
CREATE STREAM range_key_source_table_01862
(
    id uint64,
    value string,
    first Date,
    last Date
) ENGINE = Memory();

INSERT INTO range_key_source_table_01862 VALUES (1, 'First', today(), today());
INSERT INTO range_key_source_table_01862 VALUES (1, 'First', today(), today());

-- simple range_hashed
DROP DICTIONARY IF EXISTS simple_key_range_hashed_dictionary_01862;
CREATE DICTIONARY simple_key_range_hashed_dictionary_01862
(
    id uint64,
    value string,
    first Date,
    last Date
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'range_key_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_range_hashed_dictionary_01862';
SELECT dict_get('simple_key_range_hashed_dictionary_01862', 'value', to_uint64(1), today()) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_range_hashed_dictionary_01862';
SELECT dict_get('simple_key_range_hashed_dictionary_01862', 'value', to_uint64(2), today()) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'simple_key_range_hashed_dictionary_01862';

DROP DICTIONARY simple_key_range_hashed_dictionary_01862;

DROP STREAM range_key_source_table_01862;

--
-- IP Trie
--
DROP STREAM IF EXISTS ip_trie_source_table_01862;
CREATE STREAM ip_trie_source_table_01862
(
    prefix string,
    value string
) ENGINE = Memory();

INSERT INTO ip_trie_source_table_01862 VALUES ('127.0.0.0/8', 'First');
INSERT INTO ip_trie_source_table_01862 VALUES ('127.0.0.0/8', 'First');

-- ip_trie
DROP DICTIONARY IF EXISTS ip_trie_dictionary_01862;
CREATE DICTIONARY ip_trie_dictionary_01862
(
    prefix string,
    value string
)
PRIMARY KEY prefix
SOURCE(TIMEPLUS(STREAM 'ip_trie_source_table_01862' USER 'proton' PASSWORD 'proton@t+'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 0 MAX 1000);

-- found_rate = 0, because we didn't make any searches.
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'ip_trie_dictionary_01862';
-- found_rate = 1, because the dictionary covers the 127.0.0.1 address.
SELECT dict_get('ip_trie_dictionary_01862', 'value', (to_ipv4('127.0.0.1'))) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'ip_trie_dictionary_01862';
-- found_rate = 0.5, because the dictionary does not cover 1.1.1.1 and we have two lookups in total as of now.
SELECT dict_get('ip_trie_dictionary_01862', 'value', (to_ipv4('1.1.1.1'))) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'ip_trie_dictionary_01862';

DROP DICTIONARY ip_trie_dictionary_01862;

DROP STREAM ip_trie_source_table_01862;

-- Polygon
DROP STREAM IF EXISTS polygons_01862;
CREATE STREAM polygons_01862 (
    key array(array(array(tuple(float64, float64)))),
    name string
) ENGINE = Memory;
INSERT INTO polygons_01862 VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East');
INSERT INTO polygons_01862 VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North');
INSERT INTO polygons_01862 VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South');
INSERT INTO polygons_01862 VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West');

DROP STREAM IF EXISTS points_01862;
CREATE STREAM points_01862 (x float64, y float64) ENGINE = Memory;
INSERT INTO points_01862 VALUES ( 0.1,  0.0);
INSERT INTO points_01862 VALUES (-0.1,  0.0);
INSERT INTO points_01862 VALUES ( 0.0,  1.1);
INSERT INTO points_01862 VALUES ( 0.0, -1.1);
INSERT INTO points_01862 VALUES ( 3.0,  3.0);

DROP DICTIONARY IF EXISTS polygon_dictionary_01862;
CREATE DICTIONARY polygon_dictionary_01862
(
    key array(array(array(tuple(float64, float64)))),
    name string
)
PRIMARY KEY key
SOURCE(TIMEPLUS(USER 'proton' PASSWORD 'proton@t+' STREAM 'polygons_01862'))
LIFETIME(0)
LAYOUT(POLYGON());

SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'polygon_dictionary_01862';
SELECT (x, y) as key, dict_get('polygon_dictionary_01862', 'name', key) FROM points_01862 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = current_database() AND name = 'polygon_dictionary_01862';

DROP DICTIONARY polygon_dictionary_01862;
DROP STREAM polygons_01862;
DROP STREAM points_01862;
