DROP STREAM IF EXISTS 99274_backfill_historical_filter_pruning;

CREATE STREAM 99274_backfill_historical_filter_pruning(id int32)
SETTINGS flush_threshold_count = 1, index_granularity = 1;

SELECT sleep(1) FORMAT Null;

INSERT INTO 99274_backfill_historical_filter_pruning(id, _tp_time)
SELECT 1, to_datetime64('2024-01-03 00:00:00', 3, 'UTC')
UNION ALL
SELECT 2, to_datetime64('2024-02-03 00:00:00', 3, 'UTC')
UNION ALL
SELECT 3, to_datetime64('2026-04-10 02:00:00', 3, 'UTC')
UNION ALL
SELECT 4, to_datetime64('2026-04-10 03:00:00', 3, 'UTC')
UNION ALL
SELECT 5, to_datetime64('2026-04-10 04:00:00', 3, 'UTC');

SELECT sleep(3) FORMAT Null;
OPTIMIZE STREAM 99274_backfill_historical_filter_pruning FINAL;
SELECT sleep(1) FORMAT Null;

SELECT '-- query_mode=table';
SELECT trim(leading ' ' from explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM 99274_backfill_historical_filter_pruning
    WHERE _tp_time >= to_datetime64('2026-04-10 03:00:00', 3, 'UTC')
    ORDER BY id
    SETTINGS query_mode = 'table'
)
WHERE explain LIKE '%Condition:%'
   OR explain LIKE '%Name:%'
   OR explain LIKE '%Parts:%'
   OR explain LIKE '%Granules:%';

SELECT '-- backfill';
SELECT trim(leading ' ' from explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM 99274_backfill_historical_filter_pruning
    WHERE _tp_time >= to_datetime64('2026-04-10 03:00:00', 3, 'UTC')
    ORDER BY id
    SETTINGS enable_backfill_from_historical_store = 1
)
WHERE explain LIKE '%Condition:%'
   OR explain LIKE '%Name:%'
   OR explain LIKE '%Parts:%'
   OR explain LIKE '%Granules:%';

SELECT '-- backfill in order';
SELECT trim(leading ' ' from explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM 99274_backfill_historical_filter_pruning
    WHERE _tp_time >= to_datetime64('2026-04-10 03:00:00', 3, 'UTC')
    ORDER BY id
    SETTINGS enable_backfill_from_historical_store = 1, force_backfill_in_order = 1
)
WHERE explain LIKE '%Condition:%'
   OR explain LIKE '%Name:%'
   OR explain LIKE '%Parts:%'
   OR explain LIKE '%Granules:%';

DROP STREAM IF EXISTS 99274_backfill_historical_filter_pruning;
