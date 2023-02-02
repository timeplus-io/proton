SELECT
    count(),
    count_or_null(),
    sum(x),
    sum_or_null(x)
FROM
(
    SELECT number AS x
    FROM numbers(10)
    WHERE number > 10
);

SELECT
    count(),
    count_or_null(),
    sum(x),
    sum_or_null(x)
FROM
(
    SELECT 1 AS x
    WHERE 0
);
