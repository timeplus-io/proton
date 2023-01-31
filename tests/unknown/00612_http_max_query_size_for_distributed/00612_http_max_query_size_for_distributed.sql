-- Tags: no-parallel

DROP STREAM IF EXISTS data_00612;
DROP STREAM IF EXISTS dist_00612;

CREATE STREAM data_00612 (key uint64, val uint64) ENGINE = MergeTree ORDER BY key;
CREATE STREAM dist_00612 AS data_00612 ENGINE = Distributed(test_shard_localhost, currentDatabase(), data_00612, rand());

SET insert_distributed_sync=1;
SET prefer_localhost_replica=0;
SET max_query_size=29;
INSERT INTO dist_00612 VALUES(1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
SELECT key FROM dist_00612;

SET max_query_size=262144;
SET insert_distributed_sync=0;
SET prefer_localhost_replica=1;
DROP STREAM dist_00612;
DROP STREAM data_00612;
