DROP STREAM IF EXISTS test_table;
create stream test_table
(
    `col1` DateTime,
    `col2` int64,
    `col3` aggregate_function(sumMap, tuple(array(uint8), array(uint8)))
)
ENGINE = AggregatingMergeTree() ORDER BY (col1, col2);

SHOW create stream test_table;

-- regression from performance tests comparison script
DROP STREAM IF EXISTS test;
create stream test
ENGINE = Null AS
WITH (
        SELECT arrayReduce('sumMapState', [(['foo'], array_map(x -> -0., ['foo']))])
    ) AS all_metrics
SELECT
    (finalize_aggregation(arrayReduce('sumMapMergeState', [all_metrics])) AS metrics_tuple).1 AS metric_names,
    metrics_tuple.2 AS metric_values
FROM system.one;
