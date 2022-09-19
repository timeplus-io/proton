-- Tags: long, zookeeper

DROP STREAM IF EXISTS byte_identical_r1;
DROP STREAM IF EXISTS byte_identical_r2;

create stream byte_identical_r1(x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00721/byte_identical', 'r1') ORDER BY x;
create stream byte_identical_r2(x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00721/byte_identical', 'r2') ORDER BY x;

INSERT INTO byte_identical_r1(x) VALUES (1), (2), (3);
SYSTEM SYNC REPLICA byte_identical_r2;

-- Add a column with a default expression that will yield different values on different replicas.
-- Call optimize to materialize it. Replicas should compare checksums and restore consistency.
ALTER STREAM byte_identical_r1 ADD COLUMN y uint64 DEFAULT rand();
SYSTEM SYNC REPLICA byte_identical_r1;
SYSTEM SYNC REPLICA byte_identical_r2;
SET replication_alter_partitions_sync=2;
OPTIMIZE STREAM byte_identical_r1 PARTITION tuple() FINAL;

SELECT x, t1.y - t2.y FROM byte_identical_r1 t1 SEMI LEFT JOIN byte_identical_r2 t2 USING x ORDER BY x;

DROP STREAM byte_identical_r1;
DROP STREAM byte_identical_r2;
