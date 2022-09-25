EXPLAIN SYNTAX
SELECT msg, to_datetime(int_div(ms, 1000)) AS time
FROM
(
    SELECT
        'hello' AS msg,
        to_uint64(t) * 1000 AS ms
    FROM generateRandom('t datetime')
    LIMIT 10
)
ORDER BY msg, time;
