SELECT '...const maps...';

WITH map(1, 2, 3, 4) AS m SELECT m[number] FROM numbers(5);
WITH map('1', 2, '3', 4) AS m SELECT m[to_string(number)] FROM numbers(5);

WITH map(1, 2, 3, 4) AS m SELECT m[3];
WITH map('1', 2, '3', 4) AS m SELECT m['3'];

DROP STREAM IF EXISTS t_map_02014;

CREATE STREAM t_map_02014(i1 uint64, i2 int32, m1 map(uint32, string), m2 map(int8, string), m3 map(int128, string)) ENGINE = Memory;
INSERT INTO t_map_02014 VALUES (1, -1, map(1, 'foo', 2, 'bar'), map(-1, 'foo', 1, 'bar'), map(-1, 'foo', 1, 'bar'));

SELECT '...int keys...';

SELECT m1[i1], m2[i1], m3[i1] FROM t_map_02014;
SELECT m1[i2], m2[i2], m3[i2] FROM t_map_02014;

DROP STREAM IF EXISTS t_map_02014;

CREATE STREAM t_map_02014(s string, fs fixed_string(3), m1 map(string, string), m2 map(fixed_string(3), string)) ENGINE = Memory;
INSERT INTO t_map_02014 VALUES ('aaa', 'bbb', map('aaa', 'foo', 'bbb', 'bar'), map('aaa', 'foo', 'bbb', 'bar'));

SELECT '...string keys...';

SELECT m1['aaa'], m2['aaa'] FROM t_map_02014;
SELECT m1['aaa'::fixed_string(3)], m2['aaa'::fixed_string(3)] FROM t_map_02014;
SELECT m1[s], m2[s] FROM t_map_02014;
SELECT m1[fs], m2[fs] FROM t_map_02014;
SELECT length(m2['aaa'::fixed_string(4)]) FROM t_map_02014;

DROP STREAM IF EXISTS t_map_02014;
