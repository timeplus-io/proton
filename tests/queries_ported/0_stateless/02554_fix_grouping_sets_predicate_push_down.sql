-- Tags: no-object-storage

-- Specific value doesn't matter, we just need it to be fixed, because it is a part of `EXPLAIN PIPELINE` output.
SET max_threads = 8;

DROP STREAM IF EXISTS test_grouping_sets_predicate;

CREATE STREAM test_grouping_sets_predicate
(
    day_ date,
    type_1 string
)
    ENGINE=MergeTree
ORDER BY day_;

INSERT INTO test_grouping_sets_predicate SELECT
                                             to_date('2023-01-05') AS day_,
                                             'hello, world'
FROM numbers (10);

SELECT '---Explain Syntax---';
EXPLAIN SYNTAX
SELECT *
FROM
    (
        SELECT
            day_,
            if(type_1 = '', 'all', type_1) AS type_1
        FROM
            (
                SELECT
                    day_,
                    type_1
                FROM test_grouping_sets_predicate
                WHERE day_ = '2023-01-05'
                GROUP BY
                    GROUPING SETS (
                    (day_, type_1),
                    (day_))
            ) AS t
    )
WHERE type_1 = 'all';

SELECT '';
SELECT '---Explain Pipeline---';
EXPLAIN PIPELINE
SELECT *
FROM
    (
        SELECT
            day_,
            if(type_1 = '', 'all', type_1) AS type_1
        FROM
            (
                SELECT
                    day_,
                    type_1
                FROM test_grouping_sets_predicate
                WHERE day_ = '2023-01-05'
                GROUP BY
                    GROUPING SETS (
                    (day_, type_1),
                    (day_))
            ) AS t
    )
WHERE type_1 = 'all';

SELECT '';
SELECT '---Result---';
SELECT *
FROM
    (
        SELECT
            day_,
            if(type_1 = '', 'all', type_1) AS type_1
        FROM
            (
                SELECT
                    day_,
                    type_1
                FROM test_grouping_sets_predicate
                WHERE day_ = '2023-01-05'
                GROUP BY
                    GROUPING SETS (
                    (day_, type_1),
                    (day_))
            ) AS t
    )
WHERE type_1 = 'all';

SELECT '';
SELECT '---Explain Pipeline---';
EXPLAIN PIPELINE
SELECT *
FROM
    (
        SELECT
            day_,
            if(type_1 = '', 'all', type_1) AS type_1
        FROM
            (
                SELECT
                    day_,
                    type_1
                FROM test_grouping_sets_predicate
                GROUP BY
                    GROUPING SETS (
                    (day_, type_1),
                    (day_))
            ) AS t
    )
WHERE day_ = '2023-01-05';

DROP STREAM test_grouping_sets_predicate;
