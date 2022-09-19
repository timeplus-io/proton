-- Tags: no-parallel

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS v;
DROP STREAM IF EXISTS lv;

create stream t1 (key int) Engine=Memory;
create stream t2 AS t1;
DROP STREAM t2;
create stream t2 Engine=Memory AS t1;
DROP STREAM t2;
create stream t2 AS t1 Engine=Memory;
DROP STREAM t2;
create stream t3 AS numbers(10);
DROP STREAM t3;

-- live view
SET allow_experimental_live_view=1;
CREATE LIVE VIEW lv AS SELECT * FROM t1;
create stream t3 AS lv; -- { serverError 80; }
DROP STREAM lv;

-- view
CREATE VIEW v AS SELECT * FROM t1;
create stream t3 AS v; -- { serverError 80; }
DROP STREAM v;

-- dictionary
DROP DICTIONARY IF EXISTS dict;
DROP DATABASE if exists test_01056_dict_data;
CREATE DATABASE test_01056_dict_data;
create stream test_01056_dict_data.dict_data (key int, value uint16) Engine=Memory();
CREATE DICTIONARY dict
(
    `key` uint64,
    `value` uint16
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1' PORT tcpPort()
    TABLE 'dict_data' DB 'test_01056_dict_data' USER 'default' PASSWORD ''))
LIFETIME(MIN 0 MAX 0)
LAYOUT(SPARSE_HASHED());
create stream t3 AS dict; -- { serverError 80; }

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t3;
DROP DICTIONARY dict;
DROP STREAM test_01056_dict_data.dict_data;

DROP DATABASE test_01056_dict_data;

create stream t1 (x string)  AS SELECT 1;
SELECT x, to_type_name(x) FROM t1;
DROP STREAM t1;
