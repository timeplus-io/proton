CREATE STREAM 03094_grouparrysorted_dest
(
    ServiceName low_cardinality(string) CODEC(ZSTD(1)),
    -- aggregates
    SlowSpans aggregate_function(group_array_sorted(100),
        tuple(NegativeDurationNs int64, Timestamp datetime64(9), TraceId string, SpanId string)
    ) CODEC(ZSTD(1))
)
ENGINE = MergeTree()
ORDER BY (ServiceName);

CREATE STREAM 03094_grouparrysorted_src
(
    ServiceName string,
    Duration int64,
    Timestamp datetime64(9),
    TraceId string,
    SpanId string
);

CREATE MATERIALIZED VIEW 03094_grouparrysorted_mv INTO 03094_grouparrysorted_dest
AS SELECT
   ServiceName,
   group_array_sorted_state(
                         CAST(
                             (-Duration, Timestamp, TraceId, SpanId),
                             'tuple(NegativeDurationNs int64, Timestamp datetime64(9), TraceId string, SpanId string)'
                         ), 100) as SlowSpans
FROM 03094_grouparrysorted_src
GROUP BY
    ServiceName;


INSERT INTO 03094_grouparrysorted_src SELECT * FROM generate_random('ServiceName string, Duration int64, Timestamp datetime64(9), TraceId string, SpanId string, _tp_time datetime64(3), _tp_sn int64') LIMIT 500000;
