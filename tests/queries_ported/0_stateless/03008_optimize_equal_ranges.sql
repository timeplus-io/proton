DROP STREAM IF EXISTS t_optimize_equal_ranges;

CREATE STREAM t_optimize_equal_ranges (a uint64, b string, c uint64) ORDER BY a;

SET max_block_size = 1024;
SET max_bytes_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;
-- SET optimize_use_projections = 0;

INSERT INTO t_optimize_equal_ranges(a, b, c) SELECT 0, to_string(number), number FROM numbers(30000);
INSERT INTO t_optimize_equal_ranges(a, b, c) SELECT 1, to_string(number), number FROM numbers(30000);
INSERT INTO t_optimize_equal_ranges(a, b, c) SELECT 2, to_string(number), number FROM numbers(30000);

SELECT sleep(3) FORMAT Null;

SELECT a, uniq_exact(b) FROM table(t_optimize_equal_ranges) GROUP BY a ORDER BY a SETTINGS max_threads = 16;
SELECT a, uniq_exact(b) FROM table(t_optimize_equal_ranges) GROUP BY a ORDER BY a SETTINGS max_threads = 1;
SELECT a, sum(c) FROM table(t_optimize_equal_ranges) GROUP BY a ORDER BY a SETTINGS max_threads = 16;
SELECT a, sum(c) FROM table(t_optimize_equal_ranges) GROUP BY a ORDER BY a SETTINGS max_threads = 1;

-- SYSTEM FLUSH LOGS;

-- SELECT
--     used_aggregate_functions[1] AS func,
--     Settings['max_threads'] AS threads,
--     ProfileEvents['AggregationOptimizedEqualRangesOfKeys'] > 0
-- FROM system.query_log
-- WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE '%SELECT%FROM%t_optimize_equal_ranges%'
-- ORDER BY func, threads;

DROP STREAM t_optimize_equal_ranges;