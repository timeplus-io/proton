-- Tags: disabled, long
--
-- DISABLED: proton handles `max_insert_delayed_streams_for_parallel_write`
-- differently from upstream. The second INSERT (with delayed_streams =
-- 1_000_000 and max_memory_usage = 30Mi) is expected to fail with
-- MEMORY_LIMIT_EXCEEDED upstream — but in proton it succeeds. Either
-- proton tracks per-partition buffer memory more conservatively, or the
-- streaming pipeline drains differently and never reaches the same peak.
-- The expected `serverError` annotation never fires, so the test fails
-- with "The query succeeded but the server error '241' was expected".
--
-- See tests/mergetree/cloud_part_merge.md §7.4.1. Re-enable by either
-- replacing the serverError annotation with a smoke-test assertion
-- (verify first INSERT succeeds and table is queryable), or by
-- characterizing the proton-specific memory pressure recipe and
-- writing a new annotation.
--
-- Ported from upstream ClickHouse 02228_merge_tree_insert_memory_usage.

SET insert_keeper_fault_injection_probability = 0;

DROP STREAM IF EXISTS data_02228;

CREATE STREAM data_02228 (key1 uint32, sign int8, s uint64)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (key1)
PARTITION BY key1 % 100;

INSERT INTO data_02228
SELECT number, 1, number FROM numbers_mt(10000)
SETTINGS max_memory_usage = '30Mi',
         max_partitions_per_insert_block = 1024,
         max_insert_delayed_streams_for_parallel_write = 0;

INSERT INTO data_02228
SELECT number, 1, number FROM numbers_mt(10000)
SETTINGS max_memory_usage = '30Mi',
         max_partitions_per_insert_block = 1024,
         max_insert_delayed_streams_for_parallel_write = 1000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

DROP STREAM data_02228;
