-- Tags: long, no-random-merge-tree-settings

DROP STREAM IF EXISTS t_compact_vertical_merge;

CREATE STREAM t_compact_vertical_merge (id uint64, s low_cardinality(string), arr array(uint64))
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 100,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1;

INSERT INTO t_compact_vertical_merge SELECT number, to_string(number), range(number % 10) FROM numbers(40);
INSERT INTO t_compact_vertical_merge SELECT number, to_string(number), range(number % 10) FROM numbers(40);

OPTIMIZE STREAM t_compact_vertical_merge FINAL;
SYSTEM FLUSH LOGS;

-- After 80 rows (< min_rows_for_wide_part=100), the merged part stays Compact.
WITH split_by_char('_', part_name) AS name_parts,
     to_uint64(name_parts[2]) AS min_block,
     to_uint64(name_parts[3]) AS max_block
SELECT min_block, max_block, event_type, merge_algorithm, part_type FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    database = current_database() AND
    table = 't_compact_vertical_merge' AND
    min_block = 1 AND max_block = 2
ORDER BY event_time_microseconds;

INSERT INTO t_compact_vertical_merge SELECT number, to_string(number), range(number % 10) FROM numbers(40);

OPTIMIZE STREAM t_compact_vertical_merge FINAL;
SYSTEM FLUSH LOGS;

-- After 120 rows (>= min_rows_for_wide_part=100), the merged part is promoted
-- to Wide; vertical merge is chosen because the source parts qualify under
-- allow_vertical_merges_from_compact_to_wide_parts (port of upstream PR #46282).
WITH split_by_char('_', part_name) AS name_parts,
     to_uint64(name_parts[2]) AS min_block,
     to_uint64(name_parts[3]) AS max_block
SELECT min_block, max_block, event_type, merge_algorithm, part_type FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    database = current_database() AND
    table = 't_compact_vertical_merge' AND
    min_block = 1 AND max_block = 3
ORDER BY event_time_microseconds;

DROP STREAM t_compact_vertical_merge;
