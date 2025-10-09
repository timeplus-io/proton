DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t1 (key uint8) ENGINE = MergeTree ORDER BY key;
INSERT INTO t1 VALUES (1),(2);

CREATE STREAM t2 (key uint32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t2 VALUES (1),(2);

SELECT a FROM ( SELECT key + 1 as a, key FROM t1 GROUP BY key ) WHERE key FORMAT Null;

SET join_algorithm = 'full_sorting_merge';
-- SET max_rows_in_set_to_optimize_join = 0;

SELECT key FROM ( SELECT key FROM t1 GROUP BY key ) as t1 JOIN (SELECT key FROM t2) as t2 ON t1.key = t2.key WHERE key FORMAT Null;

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
