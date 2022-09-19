-- Tags: replica

SELECT
    number,
    range(number) AS arr,
    replicate(number, arr),
    replicate(to_string(number), arr),
    replicate(range(number), arr),
    replicate(array_map(x -> to_string(x), range(number)), arr)
FROM system.numbers
LIMIT 10;
