create stream IF NOT EXISTS data (sketch array(int8)) ENGINE=Memory;

INSERT INTO data VALUES ([-1,-1,-1]), ([4,-1,2]), ([0,25,-1]), ([-1,-1,7]), ([-1,-1,-1]);

SELECT max(sketch) FROM data;

SELECT maxArray(sketch) FROM data;

SELECT maxForEach(sketch) FROM data;

DROP STREAM data;


SELECT k, sumForEach(arr) FROM (SELECT number % 3 AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, sumForEach(arr) FROM (SELECT int_div(number, 3) AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, groupArrayForEach(arr) FROM (SELECT number % 3 AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, groupArrayForEach(arr) FROM (SELECT int_div(number, 3) AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, groupArrayForEach(arr) FROM (SELECT number % 3 AS k, array_map(x -> to_string(x), range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, groupArrayForEach(arr) FROM (SELECT int_div(number, 3) AS k, array_map(x -> to_string(x), range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, groupArrayForEach(arr), quantilesExactForEach(0.5, 0.9)(arr) FROM (SELECT int_div(number, 3) AS k, array_map(x -> number + x, range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT uniqForEach(x) FROM (SELECT empty_array_uint8() AS x UNION ALL SELECT [1, 2, 3] UNION ALL SELECT empty_array_uint8() UNION ALL SELECT [2, 2]);
