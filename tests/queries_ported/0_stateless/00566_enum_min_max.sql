SELECT min(x), max(x), sum(x) FROM (SELECT CAST(array_join([1, 2]) AS enum8('Hello' = 1, 'World' = 2)) AS x);
