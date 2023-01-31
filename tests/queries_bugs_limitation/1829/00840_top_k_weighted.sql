SELECT top_k_weighted(2)(x, weight), top_k(2)(x) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT array_join([('hello', 1), ('world', 1), ('goodbye', 1), ('abc', 1)]) AS t));
SELECT top_k_weighted(2)(x, weight), top_k(2)(x) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT array_join([('hello', 1), ('world', 1), ('goodbye', 2), ('abc', 1)]) AS t));
SELECT top_k_weighted(5)(n, weight) FROM (SELECT number as n, number as weight from system.numbers LIMIT 100);
