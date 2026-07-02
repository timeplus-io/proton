-- Ported from upstream ClickHouse 01606_merge_from_wide_to_compact.
-- Verifies the Wide↔Compact part-format transitions during merge — most
-- directly relevant regression test for §7.2 (PR #45681) since the
-- merge-algorithm decision now reads `future_part->part_format.part_type`
-- and the per-part check no longer gates merging mixed-format parts.
-- Uses system.parts (no part_log dependency).

DROP STREAM IF EXISTS wide_to_comp;

CREATE STREAM wide_to_comp (a int, b int, c int)
ENGINE = MergeTree()
ORDER BY a
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    merge_max_block_size = 8192,
    index_granularity = 8192,
    index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES wide_to_comp;

INSERT INTO wide_to_comp(a, b, c) SELECT number, number, number FROM numbers(100000);
INSERT INTO wide_to_comp(a, b, c) SELECT number, number, number FROM numbers(100000);
INSERT INTO wide_to_comp(a, b, c) SELECT number, number, number FROM numbers(100000);

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = current_database() AND active ORDER BY name;

ALTER STREAM wide_to_comp MODIFY SETTING min_rows_for_wide_part = 10000000;
SYSTEM START MERGES wide_to_comp;
OPTIMIZE STREAM wide_to_comp FINAL;

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = current_database() AND active ORDER BY name;
SELECT count() FROM wide_to_comp WHERE not ignore(*);

SYSTEM STOP MERGES wide_to_comp;
INSERT INTO wide_to_comp(a, b, c) SELECT number, number, number FROM numbers(100000);

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = current_database() AND active ORDER BY name;

ALTER STREAM wide_to_comp MODIFY SETTING min_rows_for_wide_part = 10000000;
SYSTEM START MERGES wide_to_comp;
OPTIMIZE STREAM wide_to_comp FINAL;

SELECT name, part_type FROM system.parts WHERE table = 'wide_to_comp' AND database = current_database() AND active ORDER BY name;
SELECT count() FROM wide_to_comp WHERE not ignore(*);

DROP STREAM wide_to_comp;
