-- Ported from upstream ClickHouse — vertical merge dedup on `ReplacingMergeTree`.
-- Uses upstream's `ENGINE = ReplacingMergeTree` syntax verbatim; storage factory
-- (`registerStorageMergeTree.cpp:527`) dispatches to StorageMergeTree, which gives
-- merge-time dedup like upstream.
--
-- proton-specific notes:
--   * `CREATE STREAM` keyword (proton replaced `TABLE` in its grammar) — engine
--     dispatch is by ENGINE name, so `ENGINE = ReplacingMergeTree` still goes to
--     StorageMergeTree.
--   * `OPTIMIZE STREAM` keyword (proton replaced `OPTIMIZE TABLE`).
--   * `SETTINGS compact_kv_stream = false` on each SELECT — proton applies a
--     read-time dedup pass for `MergingParams::Replacing` storages by default
--     (controlled by `compact_kv_stream`, default true), which would mask the
--     pre-OPTIMIZE `(N, M)` snapshot. Disabling it per-query lets us assert the
--     same `(N, M) → (N', M')` transition upstream's reference captures.

SET optimize_on_insert = 0;

DROP STREAM IF EXISTS replacing_table;

CREATE STREAM replacing_table (a uint32, b uint32, c uint32)
ENGINE = ReplacingMergeTree ORDER BY a
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    merge_max_block_size = 16;

SYSTEM STOP MERGES replacing_table;

INSERT INTO replacing_table SELECT number, number, number FROM numbers(16);
INSERT INTO replacing_table SELECT 100, number, number FROM numbers(16);

SELECT sum(a), count() FROM replacing_table SETTINGS compact_kv_stream = false;

SYSTEM START MERGES replacing_table;

OPTIMIZE STREAM replacing_table FINAL;

SELECT sum(a), count() FROM replacing_table SETTINGS compact_kv_stream = false;

DROP STREAM IF EXISTS replacing_table;

CREATE STREAM replacing_table
(
    key uint64,
    value uint64
)
ENGINE = ReplacingMergeTree
ORDER BY key
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0,
    min_bytes_for_wide_part = 0;

INSERT INTO replacing_table SELECT if(number = 8192, 8191, number), 1 FROM numbers(8193);

SELECT sum(key), count() FROM replacing_table SETTINGS compact_kv_stream = false;

OPTIMIZE STREAM replacing_table FINAL;

SELECT sum(key), count() FROM replacing_table SETTINGS compact_kv_stream = false;

DROP STREAM IF EXISTS replacing_table;
