-- Tags: no-parallel

DROP DATABASE IF EXISTS 01760_db CASCADE;
CREATE DATABASE 01760_db;

DROP STREAM IF EXISTS 01760_db.polygons;
CREATE STREAM 01760_db.polygons (key array(array(array(tuple(float64, float64)))), name string, value uint64, value_nullable nullable(uint64)) ENGINE = Memory;
INSERT INTO 01760_db.polygons VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East', 421, 421);
INSERT INTO 01760_db.polygons VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North', 422, NULL);
INSERT INTO 01760_db.polygons VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South', 423, 423);
INSERT INTO 01760_db.polygons VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West', 424, NULL);

DROP STREAM IF EXISTS 01760_db.points;
CREATE STREAM 01760_db.points (x float64, y float64, def_i uint64, def_s string) ENGINE = Memory;
INSERT INTO 01760_db.points VALUES (0.1, 0.0, 112, 'aax');
INSERT INTO 01760_db.points VALUES (-0.1, 0.0, 113, 'aay');
INSERT INTO 01760_db.points VALUES (0.0, 1.1, 114, 'aaz');
INSERT INTO 01760_db.points VALUES (0.0, -1.1, 115, 'aat');
INSERT INTO 01760_db.points VALUES (3.0, 3.0, 22, 'bb');

DROP DICTIONARY IF EXISTS 01760_db.dict_array;
CREATE DICTIONARY 01760_db.dict_array
(
    key array(array(array(tuple(float64, float64)))),
    name string DEFAULT 'qqq',
    value uint64 DEFAULT 10,
    value_nullable nullable(uint64) DEFAULT 20
)
PRIMARY KEY key
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'polygons' DB '01760_db'))
LIFETIME(0)
LAYOUT(POLYGON())
SETTINGS(dictionary_use_async_executor=1, max_threads=8)
;

SELECT 'dict_get';

SELECT (x, y) as key,
    dict_get('01760_db.dict_array', 'name', key),
    dict_get('01760_db.dict_array', 'value', key),
    dict_get('01760_db.dict_array', 'value_nullable', key)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'dict_get_or_default';

SELECT (x, y) as key,
    dict_get_or_default('01760_db.dict_array', 'name', key, 'DefaultName'),
    dict_get_or_default('01760_db.dict_array', 'value', key, 30),
    dict_get_or_default('01760_db.dict_array', 'value_nullable', key, 40)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'dict_has';

SELECT (x, y) as key,
    dict_has('01760_db.dict_array', key),
    dict_has('01760_db.dict_array', key),
    dict_has('01760_db.dict_array', key)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'check NaN or infinite point input';
SELECT (nan, inf) as key, dict_get('01760_db.dict_array', 'name', key); --{serverError BAD_ARGUMENTS}
SELECT (nan, nan) as key, dict_get('01760_db.dict_array', 'name', key); --{serverError BAD_ARGUMENTS}
SELECT (inf, nan) as key, dict_get('01760_db.dict_array', 'name', key); --{serverError BAD_ARGUMENTS}
SELECT (inf, inf) as key, dict_get('01760_db.dict_array', 'name', key); --{serverError BAD_ARGUMENTS}

DROP DICTIONARY 01760_db.dict_array;
DROP STREAM 01760_db.points;
DROP STREAM 01760_db.polygons;
DROP DATABASE 01760_db CASCADE;
