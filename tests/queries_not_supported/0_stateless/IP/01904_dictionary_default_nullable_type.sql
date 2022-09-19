-- Tags: no-parallel

DROP STREAM IF EXISTS dictionary_nullable_source_table;
create stream dictionary_nullable_source_table
(
    id uint64,
    value Nullable(int64)
) ;

DROP STREAM IF EXISTS dictionary_nullable_default_source_table;
create stream dictionary_nullable_default_source_table
(
    id uint64,
    value Nullable(uint64)
) ;

INSERT INTO dictionary_nullable_source_table VALUES (0, 0), (1, NULL);
INSERT INTO dictionary_nullable_default_source_table VALUES (2, 2), (3, NULL);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    id uint64,
    value Nullable(int64) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_nullable_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(FLAT());

SELECT 'Flat dictionary';
SELECT dictGet('flat_dictionary', 'value', to_uint64(0));
SELECT dictGet('flat_dictionary', 'value', to_uint64(1));
SELECT dictGet('flat_dictionary', 'value', to_uint64(2));
SELECT dictGetOrDefault('flat_dictionary', 'value', to_uint64(2), 2);
SELECT dictGetOrDefault('flat_dictionary', 'value', to_uint64(2), NULL);
SELECT dictGetOrDefault('flat_dictionary', 'value', id, value) FROM dictionary_nullable_default_source_table;
DROP DICTIONARY flat_dictionary;

DROP DICTIONARY IF EXISTS hashed_dictionary;
CREATE DICTIONARY hashed_dictionary
(
    id uint64,
    value Nullable(int64) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_nullable_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(HASHED());

SELECT 'Hashed dictionary';
SELECT dictGet('hashed_dictionary', 'value', to_uint64(0));
SELECT dictGet('hashed_dictionary', 'value', to_uint64(1));
SELECT dictGet('hashed_dictionary', 'value', to_uint64(2));
SELECT dictGetOrDefault('hashed_dictionary', 'value', to_uint64(2), 2);
SELECT dictGetOrDefault('hashed_dictionary', 'value', to_uint64(2), NULL);
SELECT dictGetOrDefault('hashed_dictionary', 'value', id, value) FROM dictionary_nullable_default_source_table;
DROP DICTIONARY hashed_dictionary;

DROP DICTIONARY IF EXISTS cache_dictionary;
CREATE DICTIONARY cache_dictionary
(
    id uint64,
    value Nullable(int64) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_nullable_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT 'Cache dictionary';
SELECT dictGet('cache_dictionary', 'value', to_uint64(0));
SELECT dictGet('cache_dictionary', 'value', to_uint64(1));
SELECT dictGet('cache_dictionary', 'value', to_uint64(2));
SELECT dictGetOrDefault('cache_dictionary', 'value', to_uint64(2), 2);
SELECT dictGetOrDefault('cache_dictionary', 'value', to_uint64(2), NULL);
SELECT dictGetOrDefault('cache_dictionary', 'value', id, value) FROM dictionary_nullable_default_source_table;
DROP DICTIONARY cache_dictionary;

DROP DICTIONARY IF EXISTS direct_dictionary;
CREATE DICTIONARY direct_dictionary
(
    id uint64,
    value Nullable(int64) DEFAULT NULL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_nullable_source_table'))
LAYOUT(DIRECT());

SELECT 'Direct dictionary';
SELECT dictGet('direct_dictionary', 'value', to_uint64(0));
SELECT dictGet('direct_dictionary', 'value', to_uint64(1));
SELECT dictGet('direct_dictionary', 'value', to_uint64(2));
SELECT dictGetOrDefault('direct_dictionary', 'value', to_uint64(2), 2);
SELECT dictGetOrDefault('direct_dictionary', 'value', to_uint64(2), NULL);
SELECT dictGetOrDefault('direct_dictionary', 'value', id, value) FROM dictionary_nullable_default_source_table;
DROP DICTIONARY direct_dictionary;

DROP DICTIONARY IF EXISTS ip_trie_dictionary;
CREATE DICTIONARY ip_trie_dictionary
(
    prefix string,
    value Nullable(int64) DEFAULT NULL
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(HOST 'localhost' port tcpPort() TABLE 'dictionary_nullable_source_table'))
LIFETIME(MIN 10 MAX 1000)
LAYOUT(IP_TRIE());

-- Nullable type is not supported by IPTrie dictionary
SELECT 'IPTrie dictionary';
SELECT dictGet('ip_trie_dictionary', 'value', tuple(IPv4StringToNum('127.0.0.0'))); --{serverError 1}

DROP STREAM dictionary_nullable_source_table;
DROP STREAM dictionary_nullable_default_source_table;

DROP STREAM IF EXISTS polygon_dictionary_nullable_source_table;
create stream polygon_dictionary_nullable_source_table
(
    key array(array(array(tuple(float64, float64)))),
    value Nullable(int64)
) ;

DROP STREAM IF EXISTS polygon_dictionary_nullable_default_source_table;
create stream polygon_dictionary_nullable_default_source_table
(
    key tuple(float64, float64),
    value Nullable(uint64)
) ;

INSERT INTO polygon_dictionary_nullable_source_table VALUES ([[[(0, 0), (0, 1), (1, 1), (1, 0)]]], 0), ([[[(0, 0), (0, 1.5), (1.5, 1.5), (1.5, 0)]]], NULL);
INSERT INTO polygon_dictionary_nullable_default_source_table VALUES ((2.0, 2.0), 2), ((4, 4), NULL);


DROP DICTIONARY IF EXISTS polygon_dictionary;
CREATE DICTIONARY polygon_dictionary
(
    key array(array(array(tuple(float64, float64)))),
    value Nullable(uint64) DEFAULT NULL
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'polygon_dictionary_nullable_source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(POLYGON());

SELECT 'Polygon dictionary';
SELECT dictGet('polygon_dictionary', 'value', tuple(0.5, 0.5));
SELECT dictGet('polygon_dictionary', 'value', tuple(1.5, 1.5));
SELECT dictGet('polygon_dictionary', 'value', tuple(2.0, 2.0));
SELECT dictGetOrDefault('polygon_dictionary', 'value', tuple(2.0, 2.0), 2);
SELECT dictGetOrDefault('polygon_dictionary', 'value', tuple(2.0, 2.0), NULL);
SELECT dictGetOrDefault('polygon_dictionary', 'value', key, value) FROM polygon_dictionary_nullable_default_source_table;

DROP DICTIONARY polygon_dictionary;
DROP STREAM polygon_dictionary_nullable_source_table;
DROP STREAM polygon_dictionary_nullable_default_source_table;

DROP STREAM IF EXISTS range_dictionary_nullable_source_table;
create stream range_dictionary_nullable_source_table
(
  key uint64,
  start_date date,
  end_date date,
  value Nullable(uint64)
)
;

DROP STREAM IF EXISTS range_dictionary_nullable_default_source_table;
create stream range_dictionary_nullable_default_source_table
(
    key uint64,
    value Nullable(uint64)
) ;

INSERT INTO range_dictionary_nullable_source_table VALUES (0, to_date('2019-05-05'), to_date('2019-05-20'), 0), (1, to_date('2019-05-05'), to_date('2019-05-20'), NULL);
INSERT INTO range_dictionary_nullable_default_source_table VALUES (2, 2), (3, NULL);

DROP DICTIONARY IF EXISTS range_dictionary;
CREATE DICTIONARY range_dictionary
(
  key uint64,
  start_date date,
  end_date date,
  value Nullable(uint64) DEFAULT NULL
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_dictionary_nullable_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);

SELECT 'Range dictionary';
SELECT dictGet('range_dictionary', 'value',  to_uint64(0), to_date('2019-05-15'));
SELECT dictGet('range_dictionary', 'value', to_uint64(1), to_date('2019-05-15'));
SELECT dictGet('range_dictionary', 'value', to_uint64(2), to_date('2019-05-15'));
SELECT dictGetOrDefault('range_dictionary', 'value', to_uint64(2), to_date('2019-05-15'), 2);
SELECT dictGetOrDefault('range_dictionary', 'value', to_uint64(2), to_date('2019-05-15'), NULL);
SELECT dictGetOrDefault('range_dictionary', 'value', key, to_date('2019-05-15'), value) FROM range_dictionary_nullable_default_source_table;

DROP DICTIONARY range_dictionary;
DROP STREAM range_dictionary_nullable_source_table;
DROP STREAM range_dictionary_nullable_default_source_table;
