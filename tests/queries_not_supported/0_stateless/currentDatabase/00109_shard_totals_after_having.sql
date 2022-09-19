-- Tags: shard, no-parallel, no-fasttest

SET max_rows_to_group_by = 100000;
SET max_block_size = 100001;
SET group_by_overflow_mode = 'any';

DROP STREAM IF EXISTS numbers500k;
create stream  numbers500k (number uint32) ;

INSERT INTO numbers500k SELECT number FROM system.numbers LIMIT 500000;

SET totals_mode = 'after_having_auto';
SELECT int_div(number, 2) AS k, count(), argMax(to_string(number), number) FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_inclusive';
SELECT int_div(number, 2) AS k, count(), argMax(to_string(number), number) FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'after_having_exclusive';
SELECT int_div(number, 2) AS k, count(), argMax(to_string(number), number) FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SET totals_mode = 'before_having';
SELECT int_div(number, 2) AS k, count(), argMax(to_string(number), number) FROM remote('127.0.0.{2,3}', currentDatabase(), numbers500k) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

DROP STREAM numbers500k;
