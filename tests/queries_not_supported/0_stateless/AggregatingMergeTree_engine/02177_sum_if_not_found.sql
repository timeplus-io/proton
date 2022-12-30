SELECT sum_if(1, 0);
SELECT sum_if(1, 0);
SELECT sum_if(1, 0);
SELECT sum_if(1, 0); -- { serverError 46 }

DROP STREAM IF EXISTS data;
DROP STREAM IF EXISTS agg;

create stream data
(
    `n` uint32,
    `t` DateTime
)
ENGINE = Null;

create stream agg
ENGINE = AggregatingMergeTree
ORDER BY tuple() AS
SELECT
    t,
    sum_if(n, 0)
FROM data
GROUP BY t; -- { serverError 46}

create stream agg
ENGINE = AggregatingMergeTree
ORDER BY tuple() AS
SELECT
    t,
    sum_if(n, 0)
FROM data
GROUP BY t;

DROP STREAM data;
DROP STREAM agg;
