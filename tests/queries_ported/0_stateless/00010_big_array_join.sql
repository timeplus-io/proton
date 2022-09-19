SELECT x FROM (SELECT array_join(['Hello', 'Goodbye']) AS x, [1, 2, 3] AS arr) ARRAY JOIN arr
