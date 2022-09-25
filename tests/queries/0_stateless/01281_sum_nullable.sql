SELECT sum_kahan(to_float64(number)) FROM numbers(10);
SELECT sum_kahan(to_nullable(to_float64(number))) FROM numbers(10);
SELECT sum(to_nullable(number)) FROM numbers(10);
SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT NULL);
SELECT sum(number) FROM numbers(10);
SELECT sum(number < 1000 ? NULL : number) FROM numbers(10);
