SELECT array_map(x -> number != -1 ? x : 0, arr)
FROM (SELECT number, range(number) AS arr FROM system.numbers LIMIT 10)
WHERE number % 2 = 1 AND array_exists(x -> number != -1, arr);
