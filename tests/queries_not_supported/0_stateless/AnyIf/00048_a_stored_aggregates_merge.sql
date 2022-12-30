-- Tags: not_supported, blocked_by_AnyIf_and_AggregatingMergeTree
SET query_mode = 'table';
DROP STREAM IF EXISTS stored_aggregates;

create stream stored_aggregates
(
    d   date,
    Uniq        aggregate_function(uniq, uint64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO stored_aggregates
SELECT
    to_date(to_uint16(to_date('2014-06-01')) + int_div(number, 100)) AS d,
    uniqState(int_div(number, 10)) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
)
GROUP BY d;

SELECT uniq_merge(Uniq) FROM stored_aggregates;

SELECT d, uniq_merge(Uniq) FROM stored_aggregates GROUP BY d ORDER BY d;

INSERT INTO stored_aggregates
SELECT
    to_date(to_uint16(to_date('2014-06-01')) + int_div(number, 100)) AS d,
    uniqState(int_div(number + 50, 10)) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 500, 1000
)
GROUP BY d;

SELECT uniq_merge(Uniq) FROM stored_aggregates;

SELECT d, uniq_merge(Uniq) FROM stored_aggregates GROUP BY d ORDER BY d;

OPTIMIZE STREAM stored_aggregates;

SELECT uniq_merge(Uniq) FROM stored_aggregates;

SELECT d, uniq_merge(Uniq) FROM stored_aggregates GROUP BY d ORDER BY d;

DROP STREAM stored_aggregates;

