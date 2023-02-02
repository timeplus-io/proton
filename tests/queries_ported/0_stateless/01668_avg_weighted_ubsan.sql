SELECT round(avg_weighted(x, y)) FROM (SELECT 1023 AS x, 1000000000 AS y UNION ALL SELECT 10 AS x, -9223372036854775808 AS y);
select avg_weighted(number, to_decimal128(number, 9)) from numbers(0);
SELECT avg_weighted(a, to_decimal64(c, 9)) OVER (PARTITION BY c) FROM (SELECT number AS a, number AS c FROM numbers(10));
select avg(to_decimal128(number, 9)) from numbers(0);
select avg_weighted(number, to_decimal128(0, 9)) from numbers(10);
