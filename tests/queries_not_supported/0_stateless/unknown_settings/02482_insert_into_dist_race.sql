DROP STREAM IF EXISTS tmp_02482;
DROP STREAM IF EXISTS dist_02482;

-- This test produces warning
SET send_logs_level = 'error';
SET prefer_localhost_replica=0;

CREATE STREAM tmp_02482 (i uint64, n low_cardinality(string)) ENGINE = Memory;
CREATE STREAM dist_02482(i uint64, n low_cardinality(nullable(string))) ENGINE = Distributed(test_cluster_two_shards, current_database(), tmp_02482, i);

SET insert_distributed_sync=1;

INSERT INTO dist_02482 VALUES (1, '1'), (2, '2');
INSERT INTO dist_02482 SELECT number, number FROM numbers(1000);

SET insert_distributed_sync=0;

SYSTEM STOP DISTRIBUTED SENDS dist_02482;

INSERT INTO dist_02482 VALUES (1, '1'),(2, '2');
INSERT INTO dist_02482 SELECT number, number FROM numbers(1000);

SYSTEM FLUSH DISTRIBUTED dist_02482;

DROP STREAM tmp_02482;
DROP STREAM dist_02482;
