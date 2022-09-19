DROP STREAM IF EXISTS stored_aggregates;

create stream stored_aggregates
(
    d date,
    Uniq aggregate_function(uniq, uint64)
)
ENGINE = AggregatingMergeTree(d, d, 8192);

INSERT INTO stored_aggregates
SELECT
    to_date('2014-06-01') AS d,
    uniqState(number) AS Uniq
FROM
(
    SELECT * FROM system.numbers LIMIT 1000
);

SELECT uniqMerge(Uniq) FROM stored_aggregates;

DROP STREAM stored_aggregates;
