DROP STREAM IF EXISTS mergeTree_02184;
CREATE STREAM mergeTree_02184 (id uint64, name string, dt Date) Engine=MergeTree ORDER BY id;
ALTER STREAM mergeTree_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH;
DETACH STREAM mergeTree_02184;
ATTACH STREAM mergeTree_02184;

DROP STREAM IF EXISTS distributed_02184;
CREATE STREAM distributed_02184 (id uint64, name string, dt Date) Engine=Distributed('test_cluster_two_shards', 'default', 'mergeTree_02184', rand());
ALTER STREAM distributed_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM distributed_02184;
ATTACH STREAM distributed_02184;

DROP STREAM IF EXISTS buffer_02184;
CREATE STREAM buffer_02184 (id uint64, name string, dt Date) ENGINE = Buffer(default, mergeTree_02184, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
ALTER STREAM buffer_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM buffer_02184;
ATTACH STREAM buffer_02184;

DROP STREAM IF EXISTS merge_02184;
CREATE STREAM merge_02184 (id uint64, name string, dt Date) ENGINE = Merge('default', 'distributed_02184');
ALTER STREAM merge_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM merge_02184;
ATTACH STREAM merge_02184;

DROP STREAM IF EXISTS null_02184;
CREATE STREAM null_02184 AS system.one Engine=Null();
ALTER STREAM null_02184 MODIFY COLUMN dummy int TTL now() + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM null_02184;
ATTACH STREAM null_02184;

DROP STREAM IF EXISTS file_02184;
CREATE STREAM file_02184 (id uint64, name string, dt Date) ENGINE = File(TabSeparated);
ALTER STREAM file_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM file_02184;
ATTACH STREAM file_02184;

DROP STREAM IF EXISTS memory_02184;
CREATE STREAM memory_02184 (id uint64, name string, dt Date) ENGINE = Memory();
ALTER STREAM memory_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM memory_02184;
ATTACH STREAM memory_02184;

DROP STREAM IF EXISTS log_02184;
CREATE STREAM log_02184 (id uint64, name string, dt Date) ENGINE = Log();
ALTER STREAM log_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM log_02184;
ATTACH STREAM log_02184;

DROP STREAM IF EXISTS ting_log_02184;
CREATE STREAM ting_log_02184 (id uint64, name string, dt Date) ENGINE = TinyLog();
ALTER STREAM ting_log_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM ting_log_02184;
ATTACH STREAM ting_log_02184;

DROP STREAM IF EXISTS stripe_log_02184;
CREATE STREAM stripe_log_02184 (id uint64, name string, dt Date) ENGINE = StripeLog;
ALTER STREAM stripe_log_02184 MODIFY COLUMN name string TTL dt + INTERVAL 1 MONTH; -- { serverError BAD_ARGUMENTS }
DETACH STREAM stripe_log_02184;
ATTACH STREAM stripe_log_02184;
