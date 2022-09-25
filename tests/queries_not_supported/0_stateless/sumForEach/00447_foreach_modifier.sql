create stream IF NOT EXISTS data (sketch array(int8)) ENGINE=Memory;

INSERT INTO data VALUES ([-1,-1,-1]), ([4,-1,2]), ([0,25,-1]), ([-1,-1,7]), ([-1,-1,-1]);
SELECT sleep(3);

SELECT max(sketch) FROM data;

SELECT max_array(sketch) FROM data;

SELECT max_for_each(sketch) FROM data;

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM data;


SELECT k, sum_for_each(arr) FROM (SELECT number % 3 AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, sum_for_each(arr) FROM (SELECT int_div(number, 3) AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, group_array_for_each(arr) FROM (SELECT number % 3 AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, group_array_for_each(arr) FROM (SELECT int_div(number, 3) AS k, range(number) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, group_array_for_each(arr) FROM (SELECT number % 3 AS k, array_map(x -> to_string(x), range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;
SELECT k, group_array_for_each(arr) FROM (SELECT int_div(number, 3) AS k, array_map(x -> to_string(x), range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT k, group_array_for_each(arr), quantiles_exact_for_each(0.5, 0.9)(arr) FROM (SELECT int_div(number, 3) AS k, array_map(x -> number + x, range(number)) AS arr FROM system.numbers LIMIT 10) GROUP BY k ORDER BY k;

SELECT uniq_for_each(x) FROM (SELECT empty_array_uint8() AS x UNION ALL SELECT [1, 2, 3] UNION ALL SELECT empty_array_uint8() UNION ALL SELECT [2, 2]);
