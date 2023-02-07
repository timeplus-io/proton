SELECT * FROM (SELECT * FROM (SELECT 0 AS a, to_nullable(number) AS b, to_string(number) AS c FROM numbers(1000000.)) ORDER BY a DESC, b DESC, c ASC LIMIT 1500) LIMIT 10;
