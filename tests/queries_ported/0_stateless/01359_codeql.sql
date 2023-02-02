-- In previous ClickHouse versions, the multiplications was made in a wrong type leading to overflow.
SELECT round(avg_weighted(x, y)) FROM (SELECT 0xFFFFFFFF AS x, 1000000000 AS y UNION ALL SELECT 1 AS x, 1 AS y);
