-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP STREAM IF EXISTS replicated_truncate1;
DROP STREAM IF EXISTS replicated_truncate2;

create stream replicated_truncate1 (d date, k uint64, i32 int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00623/truncate', 'r1', d, k, 8192);
create stream replicated_truncate2 (d date, k uint64, i32 int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_00623/truncate', 'r2', d, k, 8192);

SELECT '======Before Truncate======';
INSERT INTO replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA replicated_truncate2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Empty======';
TRUNCATE TABLE replicated_truncate1 SETTINGS replication_alter_partitions_sync=2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Insert Data======';
INSERT INTO replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA replicated_truncate2;

SELECT * FROM replicated_truncate1 ORDER BY k;
SELECT * FROM replicated_truncate2 ORDER BY k;

DROP STREAM IF EXISTS replicated_truncate1;
DROP STREAM IF EXISTS replicated_truncate2;
