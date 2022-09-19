-- Tags: zookeeper, no-parallel

DROP STREAM IF EXISTS versioned_collapsing_table;

create stream versioned_collapsing_table(
  d date,
  key1 uint64,
  key2 uint32,
  value string,
  sign int8,
  version uint16
)
ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/versioned_collapsing_table/{shard}', '{replica}', sign, version)
PARTITION BY d
ORDER BY (key1, key2);

INSERT INTO versioned_collapsing_table VALUES (to_date('2019-10-10'), 1, 1, 'Hello', -1, 1);

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/versioned_collapsing_table/s1' and name = 'metadata';

SELECT COUNT() FROM versioned_collapsing_table;

DETACH TABLE versioned_collapsing_table;
ATTACH TABLE versioned_collapsing_table;

SELECT COUNT() FROM versioned_collapsing_table;

DROP STREAM IF EXISTS versioned_collapsing_table;
