SELECT key2, array_reduce('uniqExactMerge', arr)
FROM
(
    SELECT
        key1 % 3 AS key2,
        group_array(state) AS arr
    FROM
    (
        SELECT
            number % 10 AS key1,
            uniqExactState(number) AS state
        FROM
        (
            SELECT *
            FROM system.numbers
            LIMIT 100
        )
        GROUP BY key1
    )
    GROUP BY key2
)
ORDER BY key2;
