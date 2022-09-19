-- Tags: long, replica, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

SET replication_alter_partitions_sync = 2;

DROP STREAM IF EXISTS attach_r1;
DROP STREAM IF EXISTS attach_r2;

create stream attach_r1 (d date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00236/01/attach', 'r1', d, d, 8192);
create stream attach_r2 (d date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00236/01/attach', 'r2', d, d, 8192);

INSERT INTO attach_r1 VALUES ('2014-01-01'), ('2014-02-01'), ('2014-03-01');

SELECT d FROM attach_r1 ORDER BY d;

ALTER STREAM attach_r2 DROP PARTITION 201402;

SELECT d FROM attach_r1 ORDER BY d;

DROP STREAM attach_r1;
DROP STREAM attach_r2;
