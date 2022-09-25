-- Tags: zookeeper, no-replicated-database, no-parallel

DROP STREAM IF EXISTS rmt;

create stream rmt (n uint64, s string) ENGINE = ReplicatedMergeTree('/clickhouse/test_01148/{shard}/{database}/{table}', '{replica}') ORDER BY n;
SHOW create stream rmt;
RENAME STREAM rmt TO rmt1;
DETACH STREAM rmt1;
ATTACH STREAM rmt1;
SHOW create stream rmt1;

create stream rmt (n uint64, s string) ENGINE = ReplicatedMergeTree('{default_path_test}{uuid}', '{default_name_test}') ORDER BY n;    -- { serverError 62 }
create stream rmt (n uint64, s string) ENGINE = ReplicatedMergeTree('{default_path_test}test_01148', '{default_name_test}') ORDER BY n;
SHOW create stream rmt;
RENAME STREAM rmt TO rmt2;   -- { serverError 48 }
DETACH STREAM rmt;
ATTACH STREAM rmt;
SHOW create stream rmt;

DROP STREAM rmt;
DROP STREAM rmt1;
