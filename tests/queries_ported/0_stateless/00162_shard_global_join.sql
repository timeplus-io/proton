-- Tags: shard, disabled

SELECT n, j1, j2 FROM (SELECT to_float64(dummy + 2) AS n FROM remote('127.0.0.{1,1}', system.one)) as jr1
GLOBAL ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10) as jr2 USING n LIMIT 10;