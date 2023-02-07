select sum_merge(y) from 
(
  select cast(x, 'aggregate_function(sum, decimal(50, 10))') as y from 
   (
     select array_reduce('sum_state', [to_decimal256('0.000001', 10), to_decimal256('1.1', 10)]) as x
   )
);

select min_merge(y) from 
(
  select cast(x, 'aggregate_function(min, decimal(18, 10))') as y from 
   (
     select array_reduce('min_state', [to_decimal64('0.000001', 10), to_decimal64('1.1', 10)]) as x
   )
);


drop stream if exists consumer_02366;
drop stream if exists producer_02366;
drop stream if exists mv_02366;

CREATE STREAM consumer_02366
(
    `id` uint16,
    `dec` aggregate_function(arg_min, decimal(24, 10), uint16)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY id
ORDER BY id;

CREATE STREAM producer_02366
(
    `id` uint16,
    `dec` string
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id;

CREATE MATERIALIZED VIEW mv_02366 TO consumer_02366 AS
SELECT
    id,
    arg_minState(dec, id) AS dec
FROM
(
    SELECT
        id,
        to_decimal128(dec, 10) AS dec
    FROM producer_02366
)
GROUP BY id;

INSERT INTO producer_02366 (*) VALUES (19, '.1');

SELECT
    id,
    finalizeAggregation(dec)
FROM consumer_02366;

drop stream consumer_02366;
drop stream producer_02366;
drop stream mv_02366;
