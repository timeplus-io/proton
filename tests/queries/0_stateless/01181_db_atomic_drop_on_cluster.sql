-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

DROP STREAM IF EXISTS test_repl ON CLUSTER test_shard_localhost SYNC;
create stream test_repl ON CLUSTER test_shard_localhost (n uint64) ENGINE ReplicatedMergeTree('/clickhouse/test_01181/{database}/test_repl','r1') ORDER BY tuple();
DETACH TABLE test_repl ON CLUSTER test_shard_localhost SYNC;
ATTACH TABLE test_repl ON CLUSTER test_shard_localhost;
DROP STREAM test_repl ON CLUSTER test_shard_localhost SYNC;
