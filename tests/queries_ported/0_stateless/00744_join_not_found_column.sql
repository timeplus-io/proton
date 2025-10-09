SET joined_subquery_requires_alias = 0;

DROP STREAM IF EXISTS test_00744;
CREATE STREAM test_00744
(
    x int32
);

INSERT INTO test_00744(x) VALUES (1);
SELECT SLEEP(3);

SELECT x
FROM
(
    SELECT
        x,
        `1`
    FROM
    (
        SELECT x, 1 FROM table(test_00744)
    )
    ALL INNER JOIN
    (
        SELECT
            count(),
            1
        FROM table(test_00744)
    ) as jss2 USING (`1`)
    LIMIT 10
);

SELECT
    x,
    `1`
FROM
(
    SELECT x, 1 FROM table(test_00744)
)
ALL INNER JOIN
(
    SELECT
        count(),
        1
    FROM table(test_00744)
) as js2 USING (`1`)
LIMIT 10;