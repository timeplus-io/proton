select *　from (　select 'table' as stream, to_int64(10) as rows, to_int64(101) as elements　union all　select 'another stream' as stream, to_int64(0) as rows, to_int64(0) as elements　)　where rows - elements <> 0;

SELECT
    label,
    number
FROM
(
    SELECT
        'a' AS label,
        number
    FROM
    (
        SELECT number
        FROM numbers(10)
    )
    UNION ALL
    SELECT
        'b' AS label,
        number
    FROM
    (
        SELECT number
        FROM numbers(10)
    )
)
WHERE number IN
(
    SELECT number
    FROM numbers(5)
) order by label, number;

SELECT NULL FROM
(SELECT [1048575, NULL] AS ax, 2147483648 AS c) as t1 ARRAY JOIN ax
INNER JOIN (SELECT NULL AS c) as t2 USING (c);

