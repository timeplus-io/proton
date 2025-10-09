-- Tags: no-parallel

DROP STREAM IF EXISTS dictionary_source_table;
CREATE STREAM dictionary_source_table
(
    id uint64,
    v1 string,
    v2 nullable(string),
    v3 nullable(uint64)
) ENGINE=Memory;

INSERT INTO dictionary_source_table VALUES (0, 'zero', 'zero', 0), (1, 'one', NULL, 1);

DROP DICTIONARY IF EXISTS flat_dictionary;
CREATE DICTIONARY flat_dictionary
(
    id uint64,
    v1 string,
    v2 nullable(string) DEFAULT NULL,
    v3 nullable(uint64)
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT 'Flat dictionary';
SELECT dict_get_or_default('flat_dictionary', ('v1', 'v2'), 0, (int_div(1, id), int_div(1, id)))
FROM dictionary_source_table;
SELECT dict_get_or_default('flat_dictionary', 'v2', id+1, int_div(NULL, id))
FROM dictionary_source_table;
SELECT dict_get_or_default('flat_dictionary', 'v3', id+1, int_div(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY flat_dictionary;


DROP DICTIONARY IF EXISTS hashed_dictionary;
CREATE DICTIONARY hashed_dictionary
(
    id uint64,
    v1 string,
    v2 nullable(string) DEFAULT NULL,
    v3 nullable(uint64)
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

SELECT 'Hashed dictionary';
SELECT dict_get_or_default('hashed_dictionary', ('v1', 'v2'), 0, (int_div(1, id), int_div(1, id)))
FROM dictionary_source_table;
SELECT dict_get_or_default('hashed_dictionary', 'v2', id+1, int_div(NULL, id))
FROM dictionary_source_table;
SELECT dict_get_or_default('hashed_dictionary', 'v3', id+1, int_div(NULL, id))
FROM dictionary_source_table;
SELECT dict_get_or_default('hashed_dictionary', 'v2', 1, int_div(1, id))
FROM dictionary_source_table;
DROP DICTIONARY hashed_dictionary;


DROP DICTIONARY IF EXISTS hashed_array_dictionary;
CREATE DICTIONARY hashed_array_dictionary
(
    id uint64,
    v1 string,
    v2 nullable(string) DEFAULT NULL,
    v3 nullable(uint64)
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED_ARRAY());

SELECT 'Hashed array dictionary';
SELECT dict_get_or_default('hashed_array_dictionary', ('v1', 'v2'), 0, (int_div(1, id), int_div(1, id)))
FROM dictionary_source_table;
SELECT dict_get_or_default('hashed_array_dictionary', 'v2', id+1, int_div(NULL, id))
FROM dictionary_source_table;
SELECT dict_get_or_default('hashed_array_dictionary', 'v3', id+1, int_div(NULL, id))
FROM dictionary_source_table;
-- Fuzzer
SELECT dict_get_or_default('hashed_array_dictionary', ('v1', 'v2'), to_uint128(0), (materialize(to_nullable(NULL)), int_div(1, id), int_div(1, id))) FROM dictionary_source_table; -- { serverError TYPE_MISMATCH }
SELECT materialize(materialize(to_low_cardinality(15))), dict_get_or_default('hashed_array_dictionary', ('v1', 'v2'), 0, (int_div(materialize(NULL), id), int_div(1, id), int_div(1, id))) FROM dictionary_source_table; -- { serverError TYPE_MISMATCH }
SELECT dict_get_or_default('hashed_array_dictionary', ('v1', 'v2'), 0, (to_nullable(NULL), int_div(1, id), int_div(1, id))) FROM dictionary_source_table; -- { serverError TYPE_MISMATCH }
DROP DICTIONARY hashed_array_dictionary;


DROP STREAM IF EXISTS range_dictionary_source_table;
CREATE STREAM range_dictionary_source_table
(
    id uint64,
    start Date,
    end nullable(Date),
    val nullable(uint64)
) ENGINE=Memory;

INSERT INTO range_dictionary_source_table VALUES (0, '2023-01-01', Null, Null), (1, '2022-11-09', '2022-12-08', 1);

DROP DICTIONARY IF EXISTS range_hashed_dictionary;
CREATE DICTIONARY range_hashed_dictionary
(
    id uint64,
    start Date,
    end nullable(Date),
    val nullable(uint64)
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'range_dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(RANGE_HASHED())
RANGE(MIN start MAX end);

SELECT 'Range hashed dictionary';
SELECT dict_get_or_default('range_hashed_dictionary', 'val', id, to_date('2023-01-02'), int_div(NULL, id))
FROM range_dictionary_source_table;
DROP DICTIONARY range_hashed_dictionary;
DROP STREAM range_dictionary_source_table;


DROP DICTIONARY IF EXISTS cache_dictionary;
CREATE DICTIONARY cache_dictionary
(
    id uint64,
    v1 string,
    v2 nullable(string) DEFAULT NULL,
    v3 nullable(uint64)
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT 'Cache dictionary';
SELECT dict_get_or_default('cache_dictionary', ('v1', 'v2'), 0, (int_div(1, id), int_div(1, id)))
FROM dictionary_source_table;
SELECT dict_get_or_default('cache_dictionary', 'v2', id+1, int_div(NULL, id))
FROM dictionary_source_table;
SELECT dict_get_or_default('cache_dictionary', 'v3', id+1, int_div(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY cache_dictionary;


DROP DICTIONARY IF EXISTS direct_dictionary;
CREATE DICTIONARY direct_dictionary
(
    id uint64,
    v1 string,
    v2 nullable(string) DEFAULT NULL,
    v3 nullable(uint64)
)
PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LAYOUT(DIRECT());

SELECT 'Direct dictionary';
SELECT dict_get_or_default('direct_dictionary', ('v1', 'v2'), 0, (int_div(1, id), int_div(1, id)))
FROM dictionary_source_table;
SELECT dict_get_or_default('direct_dictionary', 'v2', id+1, int_div(NULL, id))
FROM dictionary_source_table;
SELECT dict_get_or_default('direct_dictionary', 'v3', id+1, int_div(NULL, id))
FROM dictionary_source_table;
DROP DICTIONARY direct_dictionary;


DROP STREAM dictionary_source_table;


DROP STREAM IF EXISTS ip_dictionary_source_table;
CREATE STREAM ip_dictionary_source_table
(
    id uint64,
    prefix string,
    asn uint32,
    cca2 string
) ENGINE=Memory;

INSERT INTO ip_dictionary_source_table VALUES (0, '202.79.32.0/20', 17501, 'NP'), (1, '2620:0:870::/48', 3856, 'US'), (2, '2a02:6b8:1::/48', 13238, 'RU');

DROP DICTIONARY IF EXISTS ip_dictionary;
CREATE DICTIONARY ip_dictionary
(
    id uint64,
    prefix string,
    asn uint32,
    cca2 string
)
PRIMARY KEY prefix
SOURCE(TIMEPLUS(STREAM 'ip_dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LAYOUT(IP_TRIE)
LIFETIME(3600);

SELECT 'IP TRIE dictionary';
SELECT dict_get_or_default('ip_dictionary', 'cca2', to_ipv4('202.79.32.10'), int_div(0, id))
FROM ip_dictionary_source_table;
SELECT dict_get_or_default('ip_dictionary', ('asn', 'cca2'), ipv6_string_to_num('2a02:6b8:1::1'),
(int_div(1, id), int_div(1, id))) FROM ip_dictionary_source_table;
DROP DICTIONARY ip_dictionary;


DROP STREAM IF EXISTS polygon_dictionary_source_table;
CREATE STREAM polygon_dictionary_source_table
(
    key array(array(array(tuple(float64, float64)))),
    name nullable(string)
) ENGINE=Memory;

INSERT INTO polygon_dictionary_source_table VALUES([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'East'), ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'West');

DROP DICTIONARY IF EXISTS polygon_dictionary;
CREATE DICTIONARY polygon_dictionary
(
    key array(array(array(tuple(float64, float64)))),
    name nullable(string)
)
PRIMARY KEY key
SOURCE(TIMEPLUS(STREAM 'polygon_dictionary_source_table' USER proton PASSWORD 'proton@t+'))
LIFETIME(0)
LAYOUT(POLYGON());

DROP STREAM IF EXISTS points;
CREATE STREAM points (x float64, y float64) ENGINE=Memory;
INSERT INTO points VALUES (0.5, 0), (-0.5, 0), (10,10);

SELECT 'POLYGON dictionary';
SELECT (x, y) as key, dict_get_or_default('polygon_dictionary', 'name', key, int_div(1, y))
FROM points;

DROP STREAM points;
DROP DICTIONARY polygon_dictionary;
DROP STREAM polygon_dictionary_source_table;


-- proton: regexp_tree not implemented
-- DROP STREAM IF EXISTS regexp_dictionary_source_table;
-- CREATE STREAM regexp_dictionary_source_table
-- (
--     id uint64,
--     parent_id uint64,
--     regexp string,
--     keys   array(string),
--     values array(string)
-- ) ENGINE=Memory;
--
-- INSERT INTO regexp_dictionary_source_table VALUES (1, 0, 'Linux/(\d+[\.\d]*).+tlinux', ['name', 'version'], ['TencentOS', '\1']);
-- INSERT INTO regexp_dictionary_source_table VALUES (2, 0, '(\d+)/tclwebkit(\d+[\.\d]*)', ['name', 'version', 'comment'], ['Android', '$1', 'test $1 and $2']);
-- INSERT INTO regexp_dictionary_source_table VALUES (3, 2, '33/tclwebkit', ['version'], ['13']);
-- INSERT INTO regexp_dictionary_source_table VALUES (4, 2, '3[12]/tclwebkit', ['version'], ['12']);
-- INSERT INTO regexp_dictionary_source_table VALUES (5, 2, '3[12]/tclwebkit', ['version'], ['11']);
-- INSERT INTO regexp_dictionary_source_table VALUES (6, 2, '3[12]/tclwebkit', ['version'], ['10']);
--
-- DROP DICTIONARY IF EXISTS regexp_dict;
-- create dictionary regexp_dict
-- (
--     regexp string,
--     name string,
--     version nullable(uint64),
--     comment string default 'nothing'
-- )
-- PRIMARY KEY(regexp)
-- SOURCE(TIMEPLUS(STREAM 'regexp_dictionary_source_table' USER proton PASSWORD 'proton@t+'))
-- LIFETIME(0)
-- LAYOUT(regexp_tree);
--
-- SELECT 'Regular Expression Tree dictionary';
-- SELECT dict_get_or_default('regexp_dict', 'name', concat(to_string(number), '/tclwebkit', to_string(number)),
-- int_div(1,number)) FROM numbers(2);
-- -- Fuzzer
-- SELECT dict_get_or_default('regexp_dict', 'name', concat('/tclwebkit', to_string(number)), int_div(1, number)) FROM numbers(2); -- { serverError ILLEGAL_DIVISION }
-- DROP DICTIONARY regexp_dict;
-- DROP STREAM regexp_dictionary_source_table;
