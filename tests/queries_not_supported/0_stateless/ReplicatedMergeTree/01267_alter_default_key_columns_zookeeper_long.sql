-- Tags: long, zookeeper

DROP STREAM IF EXISTS test_alter;
create stream test_alter (x date, s string) ENGINE = MergeTree ORDER BY s PARTITION BY x;
ALTER STREAM test_alter MODIFY COLUMN s DEFAULT 'Hello';
ALTER STREAM test_alter MODIFY COLUMN x DEFAULT '2000-01-01';
DESCRIBE TABLE test_alter;
DROP STREAM test_alter;

DROP STREAM IF EXISTS test_alter_r1;
DROP STREAM IF EXISTS test_alter_r2;

create stream test_alter_r1 (x date, s string) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01267/alter', 'r1') ORDER BY s PARTITION BY x;
create stream test_alter_r2 (x date, s string) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01267/alter', 'r2') ORDER BY s PARTITION BY x;

ALTER STREAM test_alter_r1 MODIFY COLUMN s DEFAULT 'Hello' SETTINGS replication_alter_partitions_sync = 2;
ALTER STREAM test_alter_r2 MODIFY COLUMN x DEFAULT '2000-01-01' SETTINGS replication_alter_partitions_sync = 2;

DESCRIBE TABLE test_alter_r1;
DESCRIBE TABLE test_alter_r2;

SYSTEM RESTART REPLICA test_alter_r1;
SYSTEM RESTART REPLICA test_alter_r2;

DESCRIBE TABLE test_alter_r1;
DESCRIBE TABLE test_alter_r2;

DROP STREAM test_alter_r1;
DROP STREAM test_alter_r2;
