-- Test that check the correctness of the result for optimize_aggregation_in_order and projections,
-- not that this optimization will take place.

DROP STREAM IF EXISTS normal;

CREATE STREAM normal
(
    `key` uint32,
    `ts` datetime,
    `value` uint32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            value
        ORDER BY ts, key
    )
)
ENGINE = MergeTree
ORDER BY (key, ts);

INSERT INTO normal SELECT
    1,
    to_datetime('2021-12-06 00:00:00') + number,
    number
FROM numbers(100000);

SET allow_experimental_projection_optimization=1, optimize_aggregation_in_order=1, force_optimize_projection=1;

WITH to_start_of_hour(ts) AS a SELECT sum(value) as v FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY a ORDER BY v LIMIT 5;
WITH to_start_of_hour(ts) AS a SELECT sum(value) as v FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY to_start_of_hour(ts), a ORDER BY v LIMIT 5;

DROP STREAM normal;

DROP STREAM IF EXISTS agg;

CREATE STREAM agg
(
    `key` uint32,
    `ts` datetime,
    `value` uint32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            sum(value)
        GROUP BY ts, key
    )
)
ENGINE = MergeTree
ORDER BY (key, ts);

INSERT INTO agg SELECT
    1,
    to_datetime('2021-12-06 00:00:00') + number,
    number
FROM numbers(100000);

SET allow_experimental_projection_optimization=1, optimize_aggregation_in_order=1, force_optimize_projection = 1;

WITH to_start_of_hour(ts) AS a SELECT sum(value) as v FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY a ORDER BY v LIMIT 5;
WITH to_start_of_hour(ts) AS a SELECT sum(value) as v FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY to_start_of_hour(ts), a ORDER BY v LIMIT 5;

DROP STREAM agg;
