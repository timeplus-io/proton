-- Tags: distributed

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS d;

create stream t (x Enum8('abc' = 0, 'def' = 1, 'ghi' = 2)) ;
INSERT INTO t VALUES (0), (1), (2);
SELECT * FROM t;

SELECT '---';
create stream d (x Enum8('abc' = 0, 'def' = 1, 'xyz' = 2)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
SELECT * FROM d;
DROP STREAM d;

SELECT '---';
create stream d (x Enum8('abc' = 0, 'def' = 1, 'xyz' = 2)) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
SELECT * FROM d;
DROP STREAM d;

SELECT '---';
create stream d (x Enum8('abc' = 0, 'def' = 1, 'xyz' = 2)) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t);
SELECT * FROM d;
DROP STREAM d;

DROP STREAM t;
