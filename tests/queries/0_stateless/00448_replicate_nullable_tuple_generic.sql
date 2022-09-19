-- Tags: replica

SELECT x, array_join(x) FROM (SELECT materialize([1, NULL, 2]) AS x);
SELECT x, array_join(x) FROM (SELECT materialize([(1, 2), (3, 4), (5, 6)]) AS x);
SELECT x, array_join(x) FROM (SELECT materialize(array_map(x -> to_fixed_string(x, 5), ['Hello', 'world'])) AS x);
