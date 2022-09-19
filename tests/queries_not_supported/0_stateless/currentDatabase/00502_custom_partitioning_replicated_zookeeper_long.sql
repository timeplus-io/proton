-- Tags: long, replica

SET replication_alter_partitions_sync = 2;

SELECT '*** Not partitioned ***';

DROP STREAM IF EXISTS not_partitioned_replica1_00502;
DROP STREAM IF EXISTS not_partitioned_replica2_00502;
create stream not_partitioned_replica1_00502(x uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/not_partitioned_00502', '1') ORDER BY x;
create stream not_partitioned_replica2_00502(x uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/not_partitioned_00502', '2') ORDER BY x;

INSERT INTO not_partitioned_replica1_00502 VALUES (1), (2), (3);
INSERT INTO not_partitioned_replica1_00502 VALUES (4), (5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'not_partitioned_replica1_00502' AND active ORDER BY name;
SYSTEM SYNC REPLICA not_partitioned_replica1_00502;
OPTIMIZE STREAM not_partitioned_replica1_00502 PARTITION tuple() FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'not_partitioned_replica2_00502' AND active ORDER BY name;

SELECT 'Sum before DETACH PARTITION:';
SELECT sum(x) FROM not_partitioned_replica2_00502;
ALTER STREAM not_partitioned_replica1_00502 DETACH PARTITION ID 'all';
SELECT 'Sum after DETACH PARTITION:';
SELECT sum(x) FROM not_partitioned_replica2_00502;

DROP STREAM not_partitioned_replica1_00502;
DROP STREAM not_partitioned_replica2_00502;

SELECT '*** Partitioned by week ***';

DROP STREAM IF EXISTS partitioned_by_week_replica1;
DROP STREAM IF EXISTS partitioned_by_week_replica2;
create stream partitioned_by_week_replica1(d date, x uint8) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test/partitioned_by_week_00502', '1') PARTITION BY toMonday(d) ORDER BY x;
create stream partitioned_by_week_replica2(d date, x uint8) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test/partitioned_by_week_00502', '2') PARTITION BY toMonday(d) ORDER BY x;

-- 2000-01-03 belongs to a different week than 2000-01-01 and 2000-01-02
INSERT INTO partitioned_by_week_replica1 VALUES ('2000-01-01', 1), ('2000-01-02', 2), ('2000-01-03', 3);
INSERT INTO partitioned_by_week_replica1 VALUES ('2000-01-03', 4), ('2000-01-03', 5);

SELECT 'Parts before OPTIMIZE:'; -- Select parts on the first replica to avoid waiting for replication.
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_week_replica1' AND active ORDER BY name;
SYSTEM SYNC REPLICA partitioned_by_week_replica1;
OPTIMIZE STREAM partitioned_by_week_replica1 PARTITION '2000-01-03' FINAL;
SELECT 'Parts after OPTIMIZE:'; -- After OPTIMIZE with replication_alter_partitions_sync=2 replicas must be in sync.
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_week_replica2' AND active ORDER BY name;

SELECT 'Sum before DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_week_replica2;
ALTER STREAM partitioned_by_week_replica1 DROP PARTITION '1999-12-27';
SELECT 'Sum after DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_week_replica2;

DROP STREAM partitioned_by_week_replica1;
DROP STREAM partitioned_by_week_replica2;

SELECT '*** Partitioned by a (date, uint8) tuple ***';

DROP STREAM IF EXISTS partitioned_by_tuple_replica1_00502;
DROP STREAM IF EXISTS partitioned_by_tuple_replica2_00502;
create stream partitioned_by_tuple_replica1_00502(d date, x uint8, y uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/partitioned_by_tuple_00502', '1') ORDER BY x PARTITION BY (d, x);
create stream partitioned_by_tuple_replica2_00502(d date, x uint8, y uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/partitioned_by_tuple_00502', '2') ORDER BY x PARTITION BY (d, x);

INSERT INTO partitioned_by_tuple_replica1_00502 VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2), ('2000-01-02', 1, 3);
INSERT INTO partitioned_by_tuple_replica1_00502 VALUES ('2000-01-02', 1, 4), ('2000-01-01', 1, 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_tuple_replica1_00502' AND active ORDER BY name;
SYSTEM SYNC REPLICA partitioned_by_tuple_replica1_00502;
OPTIMIZE STREAM partitioned_by_tuple_replica1_00502 PARTITION ('2000-01-01', 1) FINAL;
OPTIMIZE STREAM partitioned_by_tuple_replica1_00502 PARTITION ('2000-01-02', 1) FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_tuple_replica2_00502' AND active ORDER BY name;

SELECT 'Sum before DETACH PARTITION:';
SELECT sum(y) FROM partitioned_by_tuple_replica2_00502;
ALTER STREAM partitioned_by_tuple_replica1_00502 DETACH PARTITION ID '20000101-1';
SELECT 'Sum after DETACH PARTITION:';
SELECT sum(y) FROM partitioned_by_tuple_replica2_00502;

DROP STREAM partitioned_by_tuple_replica1_00502;
DROP STREAM partitioned_by_tuple_replica2_00502;

SELECT '*** Partitioned by string ***';

DROP STREAM IF EXISTS partitioned_by_string_replica1;
DROP STREAM IF EXISTS partitioned_by_string_replica2;
create stream partitioned_by_string_replica1(s string, x uint8) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test/partitioned_by_string_00502', '1') PARTITION BY s ORDER BY x;
create stream partitioned_by_string_replica2(s string, x uint8) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test/partitioned_by_string_00502', '2') PARTITION BY s ORDER BY x;

INSERT INTO partitioned_by_string_replica1 VALUES ('aaa', 1), ('aaa', 2), ('bbb', 3);
INSERT INTO partitioned_by_string_replica1 VALUES ('bbb', 4), ('aaa', 5);

SELECT 'Parts before OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_string_replica1' AND active ORDER BY name;
SYSTEM SYNC REPLICA partitioned_by_string_replica2;
OPTIMIZE STREAM partitioned_by_string_replica2 PARTITION 'aaa' FINAL;
SELECT 'Parts after OPTIMIZE:';
SELECT partition, name FROM system.parts WHERE database = currentDatabase() AND table = 'partitioned_by_string_replica2' AND active ORDER BY name;

SELECT 'Sum before DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_string_replica2;
ALTER STREAM partitioned_by_string_replica1 DROP PARTITION 'bbb';
SELECT 'Sum after DROP PARTITION:';
SELECT sum(x) FROM partitioned_by_string_replica2;

DROP STREAM partitioned_by_string_replica1;
DROP STREAM partitioned_by_string_replica2;

SELECT '*** Table without columns with fixed size ***';

DROP STREAM IF EXISTS without_fixed_size_columns_replica1;
DROP STREAM IF EXISTS without_fixed_size_columns_replica2;
create stream without_fixed_size_columns_replica1(s string) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test/without_fixed_size_columns_00502', '1') PARTITION BY length(s) ORDER BY s;
create stream without_fixed_size_columns_replica2(s string) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test/without_fixed_size_columns_00502', '2') PARTITION BY length(s) ORDER BY s;

INSERT INTO without_fixed_size_columns_replica1 VALUES ('a'), ('aa'), ('b'), ('cc');

-- Wait for replication.
SYSTEM SYNC REPLICA without_fixed_size_columns_replica2;
OPTIMIZE STREAM without_fixed_size_columns_replica2 PARTITION 1 FINAL;

SELECT 'Parts:';
SELECT partition, name, rows FROM system.parts WHERE database = currentDatabase() AND table = 'without_fixed_size_columns_replica2' AND active ORDER BY name;

SELECT 'Before DROP PARTITION:';
SELECT * FROM without_fixed_size_columns_replica2 ORDER BY s;
ALTER STREAM without_fixed_size_columns_replica1 DROP PARTITION 1;
SELECT 'After DROP PARTITION:';
SELECT * FROM without_fixed_size_columns_replica2 ORDER BY s;

DROP STREAM without_fixed_size_columns_replica1;
DROP STREAM without_fixed_size_columns_replica2;
