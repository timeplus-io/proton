-- Regression test for #2817: CTE streaming query with LIMIT feeding into
-- aggregation returned empty results because the stream finished before
-- any periodic watermark could trigger finalization.

DROP STREAM IF EXISTS 98468_cte_agg;

CREATE STREAM 98468_cte_agg (cid uint32, val float64) SETTINGS shards = 1;

SELECT sleep(2) FORMAT Null;

INSERT INTO 98468_cte_agg (cid, val, _tp_time) VALUES (1, 10.0, '2025-01-01 00:00:01.000'), (1, 20.0, '2025-01-01 00:00:02.000'), (2, 30.0, '2025-01-01 00:00:03.000'), (3, 40.0, '2025-01-01 00:00:04.000'), (3, 50.0, '2025-01-01 00:00:05.000'), (2, 60.0, '2025-01-01 00:00:06.000');

SELECT sleep(2) FORMAT Null;

-- CTE streaming + simple count
SELECT 'cte_count';
WITH data AS (SELECT * FROM 98468_cte_agg WHERE _tp_time >= earliest_ts() LIMIT 6) SELECT count() FROM data SETTINGS max_execution_time=10, timeout_overflow_mode='break';

-- CTE streaming + GROUP BY aggregation
SELECT 'cte_groupby';
WITH data AS (SELECT * FROM 98468_cte_agg WHERE _tp_time >= earliest_ts() LIMIT 6) SELECT cid, avg(val), count() FROM data GROUP BY cid ORDER BY cid SETTINGS max_execution_time=10, timeout_overflow_mode='break';

-- Subquery streaming + count (same issue without CTE syntax)
SELECT 'subquery_count';
SELECT count() FROM (SELECT * FROM 98468_cte_agg WHERE _tp_time >= earliest_ts() LIMIT 6) SETTINGS max_execution_time=10, timeout_overflow_mode='break';

-- CTE streaming non-aggregate should still work (regression guard)
SELECT 'cte_passthrough';
WITH data AS (SELECT * FROM 98468_cte_agg WHERE _tp_time >= earliest_ts() LIMIT 6) SELECT cid FROM data ORDER BY cid SETTINGS max_execution_time=10, timeout_overflow_mode='break';

DROP STREAM IF EXISTS 98468_cte_agg;
