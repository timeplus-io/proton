-- Tags: distributed

DROP STREAM IF EXISTS numbers_100k_log;
create stream numbers_100k_log   AS SELECT * FROM system.numbers LIMIT 100000;

SELECT count() = 200000 FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_100k_log) GROUP BY number WITH TOTALS ORDER BY number LIMIT 10;

SET distributed_aggregation_memory_efficient = 1,
    group_by_two_level_threshold = 1000,
    group_by_overflow_mode = 'any',
    max_rows_to_group_by = 1000,
    totals_mode = 'after_having_auto';

SELECT count() = 200000 FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_100k_log) GROUP BY number WITH TOTALS ORDER BY number LIMIT 10;

DROP STREAM numbers_100k_log;
