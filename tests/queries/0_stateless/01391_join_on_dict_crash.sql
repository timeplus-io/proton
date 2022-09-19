-- Tags: no-parallel

DROP DATABASE IF EXISTS db_01391;
CREATE DATABASE db_01391;
USE db_01391;

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS d_src;
DROP DICTIONARY IF EXISTS d;

create stream t (click_city_id uint32, click_country_id uint32) Engine = Memory;
create stream d_src (id uint64, country_id uint8, name string) Engine = Memory;

INSERT INTO t VALUES (0, 0);
INSERT INTO d_src VALUES (0, 0, 'n');

CREATE DICTIONARY d (id uint32, country_id uint8, name string)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB 'db_01391' table 'd_src'))
LIFETIME(MIN 1 MAX 1)
LAYOUT(HASHED());

select click_country_id from t cc
left join d on to_uint32(d.id) = cc.click_city_id;

DROP DICTIONARY d;
DROP STREAM t;
DROP STREAM d_src;
DROP DATABASE IF EXISTS db_01391;
