-- Tags: no-parallel

DROP DATABASE IF EXISTS 01760_db;
CREATE DATABASE 01760_db;

DROP STREAM IF EXISTS 01760_db.polygons;
create stream 01760_db.polygons (key array(array(array(tuple(float64, float64)))), name string, value uint64, value_nullable Nullable(uint64)) ;
INSERT INTO 01760_db.polygons VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East', 421, 421);
INSERT INTO 01760_db.polygons VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North', 422, NULL);
INSERT INTO 01760_db.polygons VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South', 423, 423);
INSERT INTO 01760_db.polygons VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West', 424, NULL);

DROP STREAM IF EXISTS 01760_db.points;
create stream 01760_db.points (x float64, y float64, def_i uint64, def_s string) ;
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
    value_nullable Nullable(uint64) DEFAULT 20
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons' DB '01760_db'))
LIFETIME(0)
LAYOUT(POLYGON());

SELECT 'dictGet';

SELECT tuple(x, y) as key,
    dictGet('01760_db.dict_array', 'name', key),
    dictGet('01760_db.dict_array', 'value', key),
    dictGet('01760_db.dict_array', 'value_nullable', key)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'dictGetOrDefault';

SELECT tuple(x, y) as key,
    dictGetOrDefault('01760_db.dict_array', 'name', key, 'DefaultName'),
    dictGetOrDefault('01760_db.dict_array', 'value', key, 30),
    dictGetOrDefault('01760_db.dict_array', 'value_nullable', key, 40)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'dictHas';

SELECT tuple(x, y) as key,
    dictHas('01760_db.dict_array', key),
    dictHas('01760_db.dict_array', key),
    dictHas('01760_db.dict_array', key)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'check NaN or infinite point input';
SELECT tuple(nan, inf) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}
SELECT tuple(nan, nan) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}
SELECT tuple(inf, nan) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}
SELECT tuple(inf, inf) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}

DROP DICTIONARY 01760_db.dict_array;
DROP STREAM 01760_db.points;
DROP STREAM 01760_db.polygons;
DROP DATABASE 01760_db;
