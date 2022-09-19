DROP STREAM IF EXISTS 02183_dictionary_test_table;
create stream 02183_dictionary_test_table (id uint64) ;
INSERT INTO 02183_dictionary_test_table VALUES (0), (1);

SELECT * FROM 02183_dictionary_test_table;

DROP DICTIONARY IF EXISTS 02183_flat_dictionary;
CREATE DICTIONARY 02183_flat_dictionary
(
    id uint64
)
PRIMARY KEY id
LAYOUT(FLAT())
SOURCE(CLICKHOUSE(TABLE '02183_dictionary_test_table'))
LIFETIME(0);

SELECT 'FlatDictionary';

SELECT dictGet('02183_flat_dictionary', 'value', 0); -- {serverError 36}
SELECT dictHas('02183_flat_dictionary', 0);
SELECT dictHas('02183_flat_dictionary', 1);
SELECT dictHas('02183_flat_dictionary', 2);

SELECT * FROM 02183_flat_dictionary;

DROP DICTIONARY 02183_flat_dictionary;

DROP DICTIONARY IF EXISTS 02183_hashed_dictionary;
CREATE DICTIONARY 02183_hashed_dictionary
(
    id uint64
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(TABLE '02183_dictionary_test_table'))
LIFETIME(0);

SELECT 'HashedDictionary';

SELECT dictHas('02183_hashed_dictionary', 0);
SELECT dictHas('02183_hashed_dictionary', 1);
SELECT dictHas('02183_hashed_dictionary', 2);

SELECT * FROM 02183_hashed_dictionary;

DROP DICTIONARY 02183_hashed_dictionary;

DROP DICTIONARY IF EXISTS 02183_hashed_array_dictionary;
CREATE DICTIONARY 02183_hashed_array_dictionary
(
    id uint64
)
PRIMARY KEY id
LAYOUT(HASHED_ARRAY())
SOURCE(CLICKHOUSE(TABLE '02183_dictionary_test_table'))
LIFETIME(0);

SELECT 'HashedArrayDictionary';

SELECT dictHas('02183_hashed_array_dictionary', 0);
SELECT dictHas('02183_hashed_array_dictionary', 1);
SELECT dictHas('02183_hashed_array_dictionary', 2);

SELECT * FROM 02183_hashed_array_dictionary;

DROP DICTIONARY 02183_hashed_array_dictionary;

DROP DICTIONARY IF EXISTS 02183_cache_dictionary;
CREATE DICTIONARY 02183_cache_dictionary
(
    id uint64
)
PRIMARY KEY id
LAYOUT(CACHE(SIZE_IN_CELLS 10))
SOURCE(CLICKHOUSE(TABLE '02183_dictionary_test_table'))
LIFETIME(0);

SELECT 'CacheDictionary';

SELECT dictHas('02183_cache_dictionary', 0);
SELECT dictHas('02183_cache_dictionary', 1);
SELECT dictHas('02183_cache_dictionary', 2);

SELECT * FROM 02183_cache_dictionary;

DROP DICTIONARY 02183_cache_dictionary;

DROP DICTIONARY IF EXISTS 02183_direct_dictionary;
CREATE DICTIONARY 02183_direct_dictionary
(
    id uint64
)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(TABLE '02183_dictionary_test_table'))
LIFETIME(0);

SELECT 'DirectDictionary';

SELECT dictHas('02183_direct_dictionary', 0);
SELECT dictHas('02183_direct_dictionary', 1);
SELECT dictHas('02183_direct_dictionary', 2);

SELECT * FROM 02183_direct_dictionary;

DROP DICTIONARY 02183_direct_dictionary;

DROP STREAM 02183_dictionary_test_table;

DROP STREAM IF EXISTS ip_trie_dictionary_source_table;
create stream ip_trie_dictionary_source_table
(
    prefix string
) ;

INSERT INTO ip_trie_dictionary_source_table VALUES ('127.0.0.0');

DROP DICTIONARY IF EXISTS 02183_ip_trie_dictionary;
CREATE DICTIONARY 02183_ip_trie_dictionary
(
    prefix string
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_dictionary_source_table'))
LAYOUT(IP_TRIE())
LIFETIME(0);

SELECT 'IPTrieDictionary';

SELECT dictHas('02183_ip_trie_dictionary', tuple(IPv4StringToNum('127.0.0.0')));
SELECT dictHas('02183_ip_trie_dictionary', tuple(IPv4StringToNum('127.0.0.1')));
SELECT * FROM 02183_ip_trie_dictionary;

DROP DICTIONARY 02183_ip_trie_dictionary;
DROP STREAM ip_trie_dictionary_source_table;

DROP STREAM IF EXISTS 02183_polygon_dictionary_source_table;
create stream 02183_polygon_dictionary_source_table
(
    key array(array(array(tuple(float64, float64))))
) ;

INSERT INTO 02183_polygon_dictionary_source_table VALUES ([[[(0, 0), (0, 1), (1, 1), (1, 0)]]]);

DROP DICTIONARY IF EXISTS 02183_polygon_dictionary;
CREATE DICTIONARY 02183_polygon_dictionary
(
    key array(array(array(tuple(float64, float64))))
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE '02183_polygon_dictionary_source_table'))
LAYOUT(POLYGON(store_polygon_key_column 1))
LIFETIME(0);

SELECT 'PolygonDictionary';

SELECT dictHas('02183_polygon_dictionary', tuple(0.5, 0.5));
SELECT dictHas('02183_polygon_dictionary', tuple(1.5, 1.5));
SELECT * FROM 02183_polygon_dictionary;

DROP DICTIONARY 02183_polygon_dictionary;
DROP STREAM 02183_polygon_dictionary_source_table;

DROP STREAM IF EXISTS 02183_range_dictionary_source_table;
create stream 02183_range_dictionary_source_table
(
  key uint64,
  start uint64,
  end uint64
)
;

INSERT INTO 02183_range_dictionary_source_table VALUES(0, 0, 1);

DROP DICTIONARY IF EXISTS 02183_range_dictionary;
CREATE DICTIONARY 02183_range_dictionary
(
  key uint64,
  start uint64,
  end uint64
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE '02183_range_dictionary_source_table'))
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end)
LIFETIME(0);

SELECT 'RangeHashedDictionary';
SELECT * FROM 02183_range_dictionary;
SELECT dictHas('02183_range_dictionary', 0, 0);
SELECT dictHas('02183_range_dictionary', 0, 2);

DROP DICTIONARY 02183_range_dictionary;
DROP STREAM 02183_range_dictionary_source_table;
