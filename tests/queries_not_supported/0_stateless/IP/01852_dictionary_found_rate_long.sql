-- Tags: long, no-parallel

--
-- Simple key
--

DROP STREAM IF EXISTS simple_key_source_table_01862;
create stream simple_key_source_table_01862
(
    id uint64,
    value string
) ();

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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_source_table_01862'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT * FROM simple_key_flat_dictionary_01862 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_flat_dictionary_01862';
SELECT * FROM simple_key_flat_dictionary_01862 WHERE id = 0 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_flat_dictionary_01862';
SELECT dictGet('simple_key_flat_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, round(found_rate, 2) FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_flat_dictionary_01862';

DROP DICTIONARY simple_key_flat_dictionary_01862;

-- simple direct
DROP DICTIONARY IF EXISTS simple_key_direct_dictionary_01862;
CREATE DICTIONARY simple_key_direct_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_source_table_01862'))
LAYOUT(DIRECT());

-- check that found_rate is 0, not nan
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_direct_dictionary_01862';
SELECT * FROM simple_key_direct_dictionary_01862 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_direct_dictionary_01862';
SELECT dictGet('simple_key_direct_dictionary_01862', 'value', to_uint64(1)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_direct_dictionary_01862';
SELECT dictGet('simple_key_direct_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_direct_dictionary_01862';

DROP DICTIONARY simple_key_direct_dictionary_01862;

-- simple hashed
DROP DICTIONARY IF EXISTS simple_key_hashed_dictionary_01862;
CREATE DICTIONARY simple_key_hashed_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_source_table_01862'))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_hashed_dictionary_01862';
SELECT dictGet('simple_key_hashed_dictionary_01862', 'value', to_uint64(1)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_hashed_dictionary_01862';
SELECT dictGet('simple_key_hashed_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_hashed_dictionary_01862';

DROP DICTIONARY simple_key_hashed_dictionary_01862;

-- simple sparse_hashed
DROP DICTIONARY IF EXISTS simple_key_sparse_hashed_dictionary_01862;
CREATE DICTIONARY simple_key_sparse_hashed_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_source_table_01862'))
LAYOUT(SPARSE_HASHED())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_sparse_hashed_dictionary_01862';
SELECT dictGet('simple_key_sparse_hashed_dictionary_01862', 'value', to_uint64(1)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_sparse_hashed_dictionary_01862';
SELECT dictGet('simple_key_sparse_hashed_dictionary_01862', 'value', to_uint64(2)) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_sparse_hashed_dictionary_01862';

DROP DICTIONARY simple_key_sparse_hashed_dictionary_01862;

-- simple cache
DROP DICTIONARY IF EXISTS simple_key_cache_dictionary_01862;
CREATE DICTIONARY simple_key_cache_dictionary_01862
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'simple_key_source_table_01862'))
LAYOUT(CACHE(SIZE_IN_CELLS 100000))
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_cache_dictionary_01862';
SELECT to_uint64(1) as key, dictGet('simple_key_cache_dictionary_01862', 'value', key) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_cache_dictionary_01862';
SELECT to_uint64(2) as key, dictGet('simple_key_cache_dictionary_01862', 'value', key) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_cache_dictionary_01862';

DROP DICTIONARY simple_key_cache_dictionary_01862;

DROP STREAM simple_key_source_table_01862;

--
-- Complex key
--

DROP STREAM IF EXISTS complex_key_source_table_01862;
create stream complex_key_source_table_01862
(
    id uint64,
    id_key string,
    value string
) ();

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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'complex_key_source_table_01862'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_hashed_dictionary_01862';
SELECT dictGet('complex_key_hashed_dictionary_01862', 'value', (to_uint64(1), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_hashed_dictionary_01862';
SELECT dictGet('complex_key_hashed_dictionary_01862', 'value', (to_uint64(2), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_hashed_dictionary_01862';

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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'complex_key_source_table_01862'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_direct_dictionary_01862';
SELECT dictGet('complex_key_direct_dictionary_01862', 'value', (to_uint64(1), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_direct_dictionary_01862';
SELECT dictGet('complex_key_direct_dictionary_01862', 'value', (to_uint64(2), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_direct_dictionary_01862';

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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'complex_key_source_table_01862'))
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 100000))
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_cache_dictionary_01862';
SELECT dictGet('complex_key_cache_dictionary_01862', 'value', (to_uint64(1), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_cache_dictionary_01862';
SELECT dictGet('complex_key_cache_dictionary_01862', 'value', (to_uint64(2), 'FirstKey')) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'complex_key_cache_dictionary_01862';

DROP DICTIONARY complex_key_cache_dictionary_01862;

DROP STREAM complex_key_source_table_01862;

--
-- Range
--
DROP STREAM IF EXISTS range_key_source_table_01862;
create stream range_key_source_table_01862
(
    id uint64,
    value string,
    first date,
    last date
) ();

INSERT INTO range_key_source_table_01862 VALUES (1, 'First', today(), today());
INSERT INTO range_key_source_table_01862 VALUES (1, 'First', today(), today());

-- simple range_hashed
DROP DICTIONARY IF EXISTS simple_key_range_hashed_dictionary_01862;
CREATE DICTIONARY simple_key_range_hashed_dictionary_01862
(
    id uint64,
    value string,
    first date,
    last date
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_source_table_01862'))
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_range_hashed_dictionary_01862';
SELECT dictGet('simple_key_range_hashed_dictionary_01862', 'value', to_uint64(1), today()) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_range_hashed_dictionary_01862';
SELECT dictGet('simple_key_range_hashed_dictionary_01862', 'value', to_uint64(2), today()) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'simple_key_range_hashed_dictionary_01862';

DROP DICTIONARY simple_key_range_hashed_dictionary_01862;

DROP STREAM range_key_source_table_01862;

--
-- IP Trie
--
DROP STREAM IF EXISTS ip_trie_source_table_01862;
create stream ip_trie_source_table_01862
(
    prefix string,
    value string
) ();

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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'ip_trie_source_table_01862'))
LAYOUT(IP_TRIE())
LIFETIME(MIN 0 MAX 1000);

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_dictionary_01862';
SELECT dictGet('ip_trie_dictionary_01862', 'value', tuple(toIPv4('127.0.0.1'))) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_dictionary_01862';
SELECT dictGet('ip_trie_dictionary_01862', 'value', tuple(toIPv4('1.1.1.1'))) FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_dictionary_01862';

DROP DICTIONARY ip_trie_dictionary_01862;

DROP STREAM ip_trie_source_table_01862;

-- Polygon
DROP STREAM IF EXISTS polygons_01862;
create stream polygons_01862 (
    key array(array(array(tuple(float64, float64)))),
    name string
) ;
INSERT INTO polygons_01862 VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East');
INSERT INTO polygons_01862 VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North');
INSERT INTO polygons_01862 VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South');
INSERT INTO polygons_01862 VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West');

DROP STREAM IF EXISTS points_01862;
create stream points_01862 (x float64, y float64) ;
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
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons_01862'))
LIFETIME(0)
LAYOUT(POLYGON());

SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dictionary_01862';
SELECT tuple(x, y) as key, dictGet('polygon_dictionary_01862', 'name', key) FROM points_01862 FORMAT Null;
SELECT name, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'polygon_dictionary_01862';

DROP STREAM polygons_01862;
DROP STREAM points_01862;
DROP DICTIONARY polygon_dictionary_01862;
