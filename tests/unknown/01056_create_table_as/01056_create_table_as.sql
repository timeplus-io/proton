-- Tags: no-parallel

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS v;
DROP STREAM IF EXISTS lv;

CREATE STREAM t1 (key Int) Engine=Memory;
CREATE STREAM t2 AS t1;
DROP STREAM t2;
CREATE STREAM t2 Engine=Memory AS t1;
DROP STREAM t2;
CREATE STREAM t2 AS t1 Engine=Memory;
DROP STREAM t2;
CREATE STREAM t3 AS numbers(10);
DROP STREAM t3;

-- live view
SET allow_experimental_live_view=1;
CREATE LIVE VIEW lv AS SELECT * FROM t1;
CREATE STREAM t3 AS lv; -- { serverError 80; }
DROP STREAM lv;

-- view
CREATE VIEW v AS SELECT * FROM t1;
CREATE STREAM t3 AS v; -- { serverError 80; }
DROP STREAM v;

-- dictionary
DROP DICTIONARY IF EXISTS dict;
DROP DATABASE if exists test_01056_dict_data;
CREATE DATABASE test_01056_dict_data;
CREATE STREAM test_01056_dict_data.dict_data (key Int, value UInt16) Engine=Memory();
CREATE DICTIONARY dict
(
    `key` UInt64,
    `value` UInt16
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1' PORT tcpPort()
    STREAM 'dict_data' DB 'test_01056_dict_data' USER 'default' PASSWORD ''))
LIFETIME(MIN 0 MAX 0)
LAYOUT(SPARSE_HASHED());
CREATE STREAM t3 AS dict; -- { serverError 80; }

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t3;
DROP DICTIONARY dict;
DROP STREAM test_01056_dict_data.dict_data;

DROP DATABASE test_01056_dict_data;

CREATE STREAM t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
DROP STREAM t1;
