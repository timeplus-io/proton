DROP STREAM IF EXISTS multidimensional;
create stream multidimensional ENGINE = MergeTree ORDER BY number AS SELECT number, array_map(x -> (x, [x], [[x]], (x, to_string(x))), array_map(x -> range(x), range(number % 10))) AS value FROM system.numbers LIMIT 100000;

SELECT sum(cityHash64(to_string(value))) FROM multidimensional;

DROP STREAM multidimensional;
