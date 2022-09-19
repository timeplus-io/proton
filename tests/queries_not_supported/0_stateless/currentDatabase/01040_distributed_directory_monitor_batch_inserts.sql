-- Tags: distributed

DROP STREAM IF EXISTS test_01040;
DROP STREAM IF EXISTS dist_test_01040;

create stream test_01040 (key uint64) ();
create stream dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards, currentDatabase(), test_01040, key) SETTINGS
    monitor_batch_inserts=1,
    monitor_sleep_time_ms=10,
    monitor_max_sleep_time_ms=100;

-- internal_replication=false
SELECT 'test_cluster_two_shards prefer_localhost_replica=0';
SET prefer_localhost_replica=0;
INSERT INTO dist_test_01040 SELECT to_uint64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;

SELECT 'test_cluster_two_shards prefer_localhost_replica=1';
SET prefer_localhost_replica=1;
INSERT INTO dist_test_01040 SELECT to_uint64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;

DROP STREAM dist_test_01040;

-- internal_replication=true
create stream dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards_internal_replication, currentDatabase(), test_01040, key) SETTINGS
    monitor_batch_inserts=1,
    monitor_sleep_time_ms=10,
    monitor_max_sleep_time_ms=100;
SELECT 'test_cluster_two_shards_internal_replication prefer_localhost_replica=0';
SET prefer_localhost_replica=0;
INSERT INTO dist_test_01040 SELECT to_uint64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;

SELECT 'test_cluster_two_shards_internal_replication prefer_localhost_replica=1';
SET prefer_localhost_replica=1;
INSERT INTO dist_test_01040 SELECT to_uint64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;


DROP STREAM dist_test_01040;
DROP STREAM test_01040;
