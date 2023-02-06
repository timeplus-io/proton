drop stream if exists test;

-- #29010
CREATE STREAM test
(
    d DateTime,
    a string,
    b uint64
)
ENGINE = MergeTree
PARTITION BY to_date(d)
ORDER BY d;

SELECT *
FROM (
    SELECT
        a,
        max((d, b)).2 AS value
    FROM test
    GROUP BY rollup(a)
)
WHERE a <> '';

-- the same query, but after syntax optimization
SELECT
    a,
    value
FROM
(
    SELECT
        a,
        max((d, b)).2 AS value
    FROM test
    GROUP BY a
        WITH ROLLUP
    HAVING a != ''
)
WHERE a != '';

drop stream if exists test;
