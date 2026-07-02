-- Tags: long, no-random-merge-tree-settings
-- Adapted from upstream 03393_max_merge_delayed_streams_for_parallel_write.sql.
-- Upstream exercises the cap on an S3-backed `system.metric_log` (1200+ cols);
-- proton uses a synthetic 64-column stream on the local disk because:
--   * `system.metric_log` is not a proton built-in
--   * `system.merge_tree_settings` is not a proton built-in either
--   * local DataPartStorage returns supportParallelWrite()=false, so the cap
--     short-circuits to "no flush mid-merge" — this test instead exercises
--     that the new setting parses (DDL + ALTER) and that a many-column
--     vertical merge with it set still completes inside a reasonable bound.

DROP STREAM IF EXISTS t_delayed_streams;

CREATE STREAM t_delayed_streams (
    id uint64,
    c0  string, c1  string, c2  string, c3  string, c4  string, c5  string, c6  string, c7  string,
    c8  string, c9  string, c10 string, c11 string, c12 string, c13 string, c14 string, c15 string,
    c16 string, c17 string, c18 string, c19 string, c20 string, c21 string, c22 string, c23 string,
    c24 string, c25 string, c26 string, c27 string, c28 string, c29 string, c30 string, c31 string,
    c32 string, c33 string, c34 string, c35 string, c36 string, c37 string, c38 string, c39 string,
    c40 string, c41 string, c42 string, c43 string, c44 string, c45 string, c46 string, c47 string,
    c48 string, c49 string, c50 string, c51 string, c52 string, c53 string, c54 string, c55 string,
    c56 string, c57 string, c58 string, c59 string, c60 string, c61 string, c62 string, c63 string
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8192,
    merge_max_block_size = 8192,
    merge_max_block_size_bytes = '10M',
    max_merge_delayed_streams_for_parallel_write = 8;

INSERT INTO t_delayed_streams (id, c0, c1, c2, c3) SELECT number, repeat('a', 100), repeat('b', 100), repeat('c', 100), repeat('d', 100) FROM numbers(2000);
INSERT INTO t_delayed_streams (id, c0, c1, c2, c3) SELECT number, repeat('a', 100), repeat('b', 100), repeat('c', 100), repeat('d', 100) FROM numbers(2001);

OPTIMIZE STREAM t_delayed_streams FINAL;
SYSTEM FLUSH LOGS;

-- The vertical merge of a 64-column part must complete without OOM. We do not
-- assert a tight byte bound because local disks do not engage the delayed-streams
-- cap; this is primarily a no-regression check that the new setting and the cap
-- code path coexist with the existing merge pipeline.
SELECT 'merge_completed' AS t, merge_algorithm, peak_memory_usage < 500 * 1024 * 1024 AS within_bound
FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    database = current_database()
    AND table = 't_delayed_streams'
    AND event_type = 'MergeParts'
    AND length(merged_from) = 2;

-- ALTER … MODIFY SETTING accepts the new setting.
ALTER STREAM t_delayed_streams MODIFY SETTING max_merge_delayed_streams_for_parallel_write = 64;
SELECT 'altered' AS t;

DROP STREAM IF EXISTS t_delayed_streams;
