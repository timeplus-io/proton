-- Ported from upstream ClickHouse — vertical merge dedup on `ReplacingMergeTree(version)`.
-- Uses upstream's `ENGINE = ReplacingMergeTree(version)` syntax verbatim; storage factory
-- (`registerStorageMergeTree.cpp:527`) dispatches to StorageMergeTree, which gives
-- merge-time dedup like upstream.
--
-- Scope reduced vs upstream: the second half (`ReplicatedReplacingMergeTree` + `OPTIMIZE
-- FINAL CLEANUP` with `is_deleted` tombstones, upstream PR #51625) is omitted because:
--   - `Replicated*MergeTree` engines are not registered in proton-enterprise
--   - `OPTIMIZE FINAL CLEANUP` and `allow_experimental_replacing_merge_with_cleanup`
--     are not implemented in proton's MergeTree code path
--
-- proton-specific notes:
--   * `CREATE STREAM` keyword (proton replaced `TABLE` in its grammar).
--   * `OPTIMIZE STREAM` keyword (proton replaced `OPTIMIZE TABLE`).
--   * `SETTINGS compact_kv_stream = false` on each SELECT — proton applies a
--     read-time dedup pass for `MergingParams::Replacing` storages by default,
--     which would mask the pre-OPTIMIZE snapshot.

SET optimize_on_insert = 0;

DROP STREAM IF EXISTS tab_00577;

CREATE STREAM tab_00577 (date date, version uint64, val uint64)
ENGINE = ReplacingMergeTree(version)
PARTITION BY date
ORDER BY date
SETTINGS enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;

INSERT INTO tab_00577 VALUES ('2018-01-01', 2, 2), ('2018-01-01', 1, 1);
INSERT INTO tab_00577 VALUES ('2018-01-01', 0, 0);

SELECT * FROM tab_00577 ORDER BY version SETTINGS compact_kv_stream = false;

OPTIMIZE STREAM tab_00577 FINAL;

SELECT * FROM tab_00577 SETTINGS compact_kv_stream = false;

DROP STREAM tab_00577;
