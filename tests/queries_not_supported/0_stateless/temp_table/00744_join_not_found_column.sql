DROP TEMPORARY STREAM IF EXISTS test_00744;
CREATE TEMPORARY STREAM test_00744
(
    x int32
);

INSERT INTO test_00744 VALUES (1);

SELECT x
FROM
(
    SELECT
        x,
        1
    FROM test_00744
    ALL INNER JOIN
    (
        SELECT
            count(),
            1
        FROM test_00744
    ) jss2 USING (1)
    LIMIT 10
);

SELECT
    x,
    1
FROM test_00744
ALL INNER JOIN
(
    SELECT
        count(),
        1
    FROM test_00744
) js2 USING (1)
LIMIT 10;
