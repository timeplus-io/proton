-- Tags: no-parallel, no-backward-compatibility-check

DROP DATABASE IF EXISTS db_01391;
CREATE DATABASE db_01391;
USE db_01391;

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS d_src;
DROP DICTIONARY IF EXISTS d;

CREATE STREAM t (click_city_id uint32, click_country_id uint32) Engine = Memory;
CREATE STREAM d_src (id uint64, country_id uint8, name string) Engine = Memory;

INSERT INTO t VALUES (0, 0);
INSERT INTO d_src VALUES (0, 0, 'n');

CREATE DICTIONARY d (id uint32, country_id uint8, name string)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' DB 'db_01391' stream 'd_src'))
LIFETIME(MIN 1 MAX 1)
LAYOUT(HASHED());

SELECT click_country_id FROM t AS cc LEFT JOIN d ON to_uint32(d.id) = cc.click_city_id;
SELECT click_country_id FROM t AS cc LEFT JOIN d ON d.country_id < 99 AND d.id = cc.click_city_id;

DROP DICTIONARY d;
DROP STREAM t;
DROP STREAM d_src;
DROP DATABASE IF EXISTS db_01391;
