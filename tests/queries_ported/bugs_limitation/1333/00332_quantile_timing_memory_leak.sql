-- Tags: no-parallel, no-fasttest

SELECT quantile_timing(number) FROM (SELECT * FROM system.numbers LIMIT 10000);
SELECT floor(log2(1 + number) / log2(1.5)) AS k, count() AS c, quantile_timing(number % 10000) AS q FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k ORDER BY k;
