-- Tags: zookeeper

DROP STREAM IF EXISTS table_with_version_replicated_1;
DROP STREAM IF EXISTS table_with_version_replicated_2;

create stream table_with_version_replicated_1
(
    key uint64,
    value string,
    version uint8,
    sign int8
)
ENGINE ReplicatedVersionedCollapsingMergeTree('/clickhouse/' || currentDatabase() || '/test_01511/{shard}/t', '1_{replica}', sign, version)
ORDER BY key;

create stream table_with_version_replicated_2
(
    key uint64,
    value string,
    version uint8,
    sign int8
)
ENGINE ReplicatedVersionedCollapsingMergeTree('/clickhouse/' || currentDatabase() || '/test_01511/{shard}/t', '2_{replica}', sign, version)
ORDER BY key;

INSERT INTO table_with_version_replicated_1 VALUES (1, '1', 1, -1);
INSERT INTO table_with_version_replicated_1 VALUES (2, '2', 2, -1);

SELECT * FROM table_with_version_replicated_1 ORDER BY key;

SHOW create stream table_with_version_replicated_1;

ALTER STREAM table_with_version_replicated_1 MODIFY COLUMN version uint32 SETTINGS replication_alter_partitions_sync=2;

SELECT * FROM table_with_version_replicated_1 ORDER BY key;

SHOW create stream table_with_version_replicated_1;

INSERT INTO TABLE table_with_version_replicated_1 VALUES(1, '1', 1, 1);
INSERT INTO TABLE table_with_version_replicated_1 VALUES(1, '1', 2, 1);

SELECT * FROM table_with_version_replicated_1 FINAL ORDER BY key;

INSERT INTO TABLE table_with_version_replicated_1 VALUES(3, '3', 65555, 1);

SELECT * FROM table_with_version_replicated_1 FINAL ORDER BY key;

INSERT INTO TABLE table_with_version_replicated_1 VALUES(3, '3', 65555, -1);

SYSTEM SYNC REPLICA table_with_version_replicated_2;

DETACH TABLE table_with_version_replicated_1;
DETACH TABLE table_with_version_replicated_2;
ATTACH TABLE table_with_version_replicated_2;
ATTACH TABLE table_with_version_replicated_1;

SELECT * FROM table_with_version_replicated_1 FINAL ORDER BY key;

SYSTEM SYNC REPLICA table_with_version_replicated_2;

SHOW create stream table_with_version_replicated_2;

SELECT * FROM table_with_version_replicated_2 FINAL ORDER BY key;

DROP STREAM IF EXISTS table_with_version_replicated_1;
DROP STREAM IF EXISTS table_with_version_replicated_2;
