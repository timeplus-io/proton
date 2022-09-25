DROP STREAM IF EXISTS tmp_01683;
DROP STREAM IF EXISTS dist_01683;

SET prefer_localhost_replica=0;
-- To suppress "Structure does not match (remote: n int8 int8(size = 0), local: n uint64 uint64(size = 1)), implicit conversion will be done."
SET send_logs_level='error';

create stream tmp_01683 (n int8) ENGINE=Memory;
create stream dist_01683 (n uint64) Engine=Distributed(test_cluster_two_shards, currentDatabase(), tmp_01683, n);

SET insert_distributed_sync=1;
INSERT INTO dist_01683 VALUES (1),(2);

SET insert_distributed_sync=0;
INSERT INTO dist_01683 VALUES (1),(2);
SYSTEM FLUSH DISTRIBUTED dist_01683;

-- TODO: cover distributed_directory_monitor_batch_inserts=1

SELECT * FROM tmp_01683 ORDER BY n;

DROP STREAM tmp_01683;
DROP STREAM dist_01683;
