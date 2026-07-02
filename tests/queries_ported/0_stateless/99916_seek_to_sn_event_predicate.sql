-- Issue 11839: verify merged settings for historical-store participation and hole handling
-- enable_backfill_from_historical_store default true: historical store may participate in streaming reads.
-- throw_if_sequence_has_hole default false: ad-hoc queries skip unrecoverable gaps silently.
SELECT name, value
FROM system.settings
WHERE name IN ('enable_backfill_from_historical_store', 'throw_if_sequence_has_hole')
ORDER BY name;

-- The legacy name allow_fallback_to_historical_store is kept as an alias of
-- enable_backfill_from_historical_store for backwards compatibility; changing one must move the other.
SET allow_fallback_to_historical_store = 0;
SELECT value FROM system.settings WHERE name = 'enable_backfill_from_historical_store';
SET allow_fallback_to_historical_store = 1;

-- Issue 11839: _tp_sn predicate with values below LogStartSN must not throw (Issue 1)
-- Verify table-mode queries with boundary SN values work correctly.
DROP STREAM IF EXISTS test_11839_seek_sn;
CREATE STREAM test_11839_seek_sn (i int, s string) SETTINGS logstore_retention_bytes = 107374182;
INSERT INTO test_11839_seek_sn (i, s) VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT sleep(3) FORMAT Null;

-- _tp_sn > 0: all rows (0 is clamped to LogStartSN=1, all SNs satisfy > 0)
SELECT i, s FROM table(test_11839_seek_sn) WHERE _tp_sn > 0 ORDER BY i;

-- _tp_sn >= 0: all rows
SELECT i, s FROM table(test_11839_seek_sn) WHERE _tp_sn >= 0 ORDER BY i;

-- _tp_sn = 0: empty (no row has SN=0)
SELECT i, s FROM table(test_11839_seek_sn) WHERE _tp_sn = 0;

-- _tp_sn > -1: all rows (-1 is clamped to LogStartSN=1)
SELECT i, s FROM table(test_11839_seek_sn) WHERE _tp_sn > -1 ORDER BY i;

DROP STREAM IF EXISTS test_11839_seek_sn;
