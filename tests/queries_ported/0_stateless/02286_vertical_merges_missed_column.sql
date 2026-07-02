-- Ported from upstream ClickHouse 02286_vertical_merges_missed_column.
-- Edge cases for vertical merge: ADD COLUMN and CLEAR COLUMN between
-- inserts and OPTIMIZE FINAL, ensuring the merge correctly synthesizes
-- defaults for columns missing in some source parts. Catches regressions
-- in the chooseMergeAlgorithm + MergeTreeSequentialSource paths touched
-- by §7.2.

DROP STREAM IF EXISTS t_vertical_merges;

CREATE STREAM t_vertical_merges
(
  a   nullable(string),
  b   int8
)
ENGINE = MergeTree()
ORDER BY ()
SETTINGS
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    min_bytes_for_wide_part = 0;

INSERT INTO t_vertical_merges(a, b) SELECT NULL, 1;
ALTER STREAM t_vertical_merges ADD COLUMN c string;
OPTIMIZE STREAM t_vertical_merges FINAL;
SELECT a, b, c FROM t_vertical_merges;

DROP STREAM IF EXISTS t_vertical_merges;

CREATE STREAM t_vertical_merges
(
  a   array(int16),
  b   int8
)
ENGINE = MergeTree()
ORDER BY ()
SETTINGS
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    min_bytes_for_wide_part = 0;

INSERT INTO t_vertical_merges(a, b) SELECT [], 1;
ALTER STREAM t_vertical_merges CLEAR COLUMN b;
OPTIMIZE STREAM t_vertical_merges FINAL;
SELECT a, b FROM t_vertical_merges;

DROP STREAM IF EXISTS t_vertical_merges;
