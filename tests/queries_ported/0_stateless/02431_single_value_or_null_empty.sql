select single_value_or_null(number) from numbers(0) with totals;

SELECT
        0.5 IN (
        SELECT single_value_or_null(*)
        FROM
            (
                SELECT 1048577
                FROM numbers(0)
            )
WITH TOTALS
    ),
    NULL,
    NULL NOT IN (
SELECT
    2147483647,
    1024 IN (
    SELECT
    [NULL, 2147483648, NULL, NULL],
    number
    FROM numbers(7, 100)
    ),
    [NULL, NULL, NULL, NULL, NULL],
    number
FROM numbers(1048576)
WHERE NULL
    ),
    NULL NOT IN (
SELECT number
FROM numbers(0)
    )
GROUP BY NULL
WITH CUBE;

SELECT any_heavy('1') FROM (SELECT any_heavy(1));
