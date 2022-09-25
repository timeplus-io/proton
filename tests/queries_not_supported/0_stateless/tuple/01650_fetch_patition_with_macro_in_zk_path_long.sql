-- Tags: long

DROP STREAM IF EXISTS test_01640;
DROP STREAM IF EXISTS restore_01640;

create stream test_01640(i int64, d date, s string)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{shard}/tables/test_01640','{replica}') 
PARTITION BY toYYYYMM(d) ORDER BY i;

insert into test_01640 values (1, '2021-01-01','some');

create stream restore_01640(i int64, d date, s string)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{shard}/tables/restore_01640','{replica}')
PARTITION BY toYYYYMM(d) ORDER BY i;

ALTER STREAM restore_01640 FETCH PARTITION tuple(toYYYYMM(to_date('2021-01-01')))
  FROM '/clickhouse/{database}/{shard}/tables/test_01640';

SELECT partition_id
FROM system.detached_parts
WHERE (table = 'restore_01640') AND (database = currentDatabase());

ALTER STREAM restore_01640 ATTACH PARTITION tuple(toYYYYMM(to_date('2021-01-01')));

SELECT partition_id
FROM system.detached_parts
WHERE (table = 'restore_01640') AND (database = currentDatabase());

SELECT _part, * FROM restore_01640;

DROP STREAM test_01640;
DROP STREAM restore_01640;


