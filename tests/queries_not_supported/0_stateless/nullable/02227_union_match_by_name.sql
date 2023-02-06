-- { echo }
EXPLAIN header = 1, optimize = 0 SELECT avg_weighted(x, y) FROM (SELECT NULL, 255 AS x, 1 AS y UNION ALL SELECT y, NULL AS x, 1 AS y);
SELECT avg_weighted(x, y) FROM (SELECT NULL, 255 AS x, 1 AS y UNION ALL SELECT y, NULL AS x, 1 AS y);
