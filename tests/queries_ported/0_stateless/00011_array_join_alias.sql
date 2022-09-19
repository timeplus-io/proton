SELECT x, a FROM (SELECT array_join(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN; -- { serverError 42 }
SELECT x, a FROM (SELECT array_join(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN arr AS a;
