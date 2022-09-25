DROP STREAM IF EXISTS t_02156_mt1;
DROP STREAM IF EXISTS t_02156_mt2;
DROP STREAM IF EXISTS t_02156_log;
DROP STREAM IF EXISTS t_02156_dist;
DROP STREAM IF EXISTS t_02156_merge1;
DROP STREAM IF EXISTS t_02156_merge2;
DROP STREAM IF EXISTS t_02156_merge3;

create stream t_02156_mt1 (k uint32, v string) ENGINE = MergeTree ORDER BY k;
create stream t_02156_mt2 (k uint32, v string) ENGINE = MergeTree ORDER BY k;
create stream t_02156_log (k uint32, v string)  ;

create stream t_02156_dist (k uint32, v string) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_02156_mt1);

create stream t_02156_merge1 (k uint32, v string) ENGINE = Merge(currentDatabase(), 't_02156_mt1|t_02156_mt2');
create stream t_02156_merge2 (k uint32, v string) ENGINE = Merge(currentDatabase(), 't_02156_mt1|t_02156_log');
create stream t_02156_merge3 (k uint32, v string) ENGINE = Merge(currentDatabase(), 't_02156_mt2|t_02156_dist');

INSERT INTO t_02156_mt1 SELECT number, to_string(number) FROM numbers(10000);
INSERT INTO t_02156_mt2 SELECT number, to_string(number) FROM numbers(10000);
INSERT INTO t_02156_log SELECT number, to_string(number) FROM numbers(10000);

EXPLAIN SYNTAX SELECT count() FROM t_02156_merge1 WHERE k = 3 AND not_empty(v);
SELECT count() FROM t_02156_merge1 WHERE k = 3 AND not_empty(v);

EXPLAIN SYNTAX SELECT count() FROM t_02156_merge2 WHERE k = 3 AND not_empty(v);
SELECT count() FROM t_02156_merge2 WHERE k = 3 AND not_empty(v);

EXPLAIN SYNTAX SELECT count() FROM t_02156_merge3 WHERE k = 3 AND not_empty(v);
SELECT count() FROM t_02156_merge3 WHERE k = 3 AND not_empty(v);

DROP STREAM IF EXISTS t_02156_mt1;
DROP STREAM IF EXISTS t_02156_mt2;
DROP STREAM IF EXISTS t_02156_log;
DROP STREAM IF EXISTS t_02156_dist;
DROP STREAM IF EXISTS t_02156_merge1;
DROP STREAM IF EXISTS t_02156_merge2;
DROP STREAM IF EXISTS t_02156_merge3;
