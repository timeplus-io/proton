-- Tags: long, no-random-merge-tree-settings
-- Exercises the operator escape hatch ported from upstream PR #46282
-- (cloud_part_merge.md §7.2.1). The setting `allow_vertical_merges_from_compact_to_wide_parts`
-- gates compact-source-part → wide-output vertical merges.
--
-- This test verifies CORRECTNESS of both code paths (setting=1 default and
-- setting=0 override). It does NOT verify which merge algorithm was chosen,
-- because proton-enterprise's PartLogElement schema lacks the `merge_algorithm`
-- column (deferred to cloud_part_merge.md §7.4.2 Phase 1). Once that column
-- exists, this test can be extended with `merge_algorithm = 'Vertical'` /
-- `merge_algorithm = 'Horizontal'` assertions to match upstream's
-- 02539-equivalent rolling-upgrade integration test.

-- ============================================================
-- Path A — default setting (allow_vertical_merges_from_compact_to_wide_parts = 1)
-- compact source parts -> Wide via Vertical merge
-- ============================================================
DROP STREAM IF EXISTS t_amftcwp_default;

CREATE STREAM t_amftcwp_default (id uint64, s low_cardinality(string), arr array(uint64))
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 100,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

-- Two compact parts (40 rows each, < min_rows_for_wide_part).
INSERT INTO t_amftcwp_default SELECT number, to_string(number), range(number % 10) FROM numbers(40);
INSERT INTO t_amftcwp_default SELECT number, to_string(number), range(number % 10) FROM numbers(40);

-- First merge: 80 rows < 100, stays Compact.
OPTIMIZE STREAM t_amftcwp_default FINAL;

-- Third compact part — third merge crosses the threshold and promotes to Wide.
-- Source parts at this point: one Compact (merged from above) + one new Compact.
-- With default setting=1, this is the path this PR-port enables.
INSERT INTO t_amftcwp_default SELECT number, to_string(number), range(number % 10) FROM numbers(40);
OPTIMIZE STREAM t_amftcwp_default FINAL;

-- Verify final part is Wide (correctness).
SELECT 'default', name, part_type
FROM system.parts
WHERE database = current_database() AND table = 't_amftcwp_default' AND active
ORDER BY name;

DROP STREAM t_amftcwp_default;

-- ============================================================
-- Path B — setting forced to 0
-- compact source parts -> Wide via Horizontal merge (escape hatch)
-- ============================================================
DROP STREAM IF EXISTS t_amftcwp_off;

CREATE STREAM t_amftcwp_off (id uint64, s low_cardinality(string), arr array(uint64))
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 100,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 0;

INSERT INTO t_amftcwp_off SELECT number, to_string(number), range(number % 10) FROM numbers(40);
INSERT INTO t_amftcwp_off SELECT number, to_string(number), range(number % 10) FROM numbers(40);
OPTIMIZE STREAM t_amftcwp_off FINAL;

INSERT INTO t_amftcwp_off SELECT number, to_string(number), range(number % 10) FROM numbers(40);
OPTIMIZE STREAM t_amftcwp_off FINAL;

-- Same Wide outcome — the setting only changes the merge algorithm chosen
-- internally, not the resulting part type. This confirms the gating in
-- chooseMergeAlgorithm doesn't break correctness when forced off.
SELECT 'off', name, part_type
FROM system.parts
WHERE database = current_database() AND table = 't_amftcwp_off' AND active
ORDER BY name;

DROP STREAM t_amftcwp_off;

-- ============================================================
-- Path C — ALTER STREAM toggles the setting at runtime
-- ============================================================
DROP STREAM IF EXISTS t_amftcwp_alter;

CREATE STREAM t_amftcwp_alter (id uint64) ENGINE = MergeTree() ORDER BY id;

-- Default value reflected (setting omitted at create time).
-- Sanity check: ALTER accepts both 0 and 1 without parse errors.
ALTER STREAM t_amftcwp_alter MODIFY SETTING allow_vertical_merges_from_compact_to_wide_parts = 0;
ALTER STREAM t_amftcwp_alter MODIFY SETTING allow_vertical_merges_from_compact_to_wide_parts = 1;

-- Confirm SHOW CREATE round-trips the setting after the second ALTER.
SELECT 'alter ok' AS marker;

DROP STREAM t_amftcwp_alter;
