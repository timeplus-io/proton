SELECT * FROM (SELECT range(number) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT array_map(x -> to_nullable(x), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT array_map(x -> (x, x), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT array_map(x -> (x, x + 1), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT array_map(x -> (x, to_nullable(x)), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
SELECT * FROM (SELECT array_map(x -> (x, null_if(x, 3)), range(number)) AS x FROM system.numbers LIMIT 10) WHERE length(x) % 2 = 0;
