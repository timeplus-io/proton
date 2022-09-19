DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS d;

create stream t (a string, b int) ;
INSERT INTO t VALUES ('a', 0), ('a', 1), ('b', 0);
SELECT * FROM t;

SELECT '---';
create stream d (a string, b int) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
SELECT DISTINCT b FROM (SELECT a, b FROM d GROUP BY a, b) order by b;
DROP STREAM d;

SELECT '---';
create stream d (a string, b int) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t);
SELECT DISTINCT b FROM (SELECT a, b FROM d GROUP BY a, b) order by b;
DROP STREAM d;

DROP STREAM t;
