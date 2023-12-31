DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t1 (x uint64, y uint64) ENGINE = MergeTree ORDER BY y
AS SELECT sipHash64(number, 't1_x') % 100 AS x, sipHash64(number, 't1_y') % 100 AS y FROM numbers(100);

CREATE STREAM t2 (x uint64, y uint64) ENGINE = MergeTree ORDER BY y
AS SELECT sipHash64(number, 't2_x') % 100 AS x, sipHash64(number, 't2_y') % 100 AS y FROM numbers(100);

SET max_rows_in_set_to_optimize_join = 1000;
SET join_algorithm = 'full_sorting_merge';

-- different combinations of conditions on key/attribute columns for the left/right streams
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x;
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x WHERE t1.y % 2 == 0;
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x WHERE t1.x % 2 == 0;
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x WHERE t2.y % 2 == 0;
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x WHERE t2.x % 2 == 0;
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x WHERE t1.y % 2 == 0 AND t2.y % 2 == 0;
SELECT count() FROM t1 JOIN t2 ON t1.x = t2.x WHERE t1.x % 2 == 0 AND t2.x % 2 == 0 AND t1.y % 2 == 0 AND t2.y % 2 == 0;
