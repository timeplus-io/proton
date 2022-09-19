-- Tags: not_supported, blocked_by_SummingMergeTree
DROP STREAM IF EXISTS summing_mt_aggregating_column;

create stream summing_mt_aggregating_column
(
    Key uint64,
    Value uint64,
    ConcatArraySimple SimpleAggregateFunction(groupArrayArray, array(uint64)),
    ConcatArrayComplex aggregate_function(groupArrayArray, array(uint64))
)
ENGINE = SummingMergeTree()
ORDER BY Key;

INSERT INTO summing_mt_aggregating_column SELECT 1, 2, [333, 444], groupArrayArrayState([to_uint64(33), to_uint64(44)]);
INSERT INTO summing_mt_aggregating_column SELECT 1, 3, [555, 999], groupArrayArrayState([to_uint64(55), to_uint64(99)]);

OPTIMIZE STREAM summing_mt_aggregating_column FINAL;

SELECT Key, any(Value), any(ConcatArraySimple), groupArrayArrayMerge(ConcatArrayComplex) FROM summing_mt_aggregating_column GROUP BY Key;

DROP STREAM IF EXISTS summing_mt_aggregating_column;
