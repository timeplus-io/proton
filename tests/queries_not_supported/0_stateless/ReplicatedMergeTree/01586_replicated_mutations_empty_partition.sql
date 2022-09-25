-- Tags: replica

DROP STREAM IF EXISTS replicated_mutations_empty_partitions;

create stream replicated_mutations_empty_partitions
(
    key uint64,
    value string
)
ENGINE = ReplicatedMergeTree('/clickhouse/test/'||currentDatabase()||'/01586_replicated_mutations_empty_partitions/{shard}', '{replica}')
ORDER BY key
PARTITION by key;

INSERT INTO replicated_mutations_empty_partitions SELECT number, to_string(number) FROM numbers(10);

SELECT count(distinct value) FROM replicated_mutations_empty_partitions;

SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/test/'||currentDatabase()||'/01586_replicated_mutations_empty_partitions/s1/block_numbers';

ALTER STREAM replicated_mutations_empty_partitions DROP PARTITION '3';
ALTER STREAM replicated_mutations_empty_partitions DROP PARTITION '4';
ALTER STREAM replicated_mutations_empty_partitions DROP PARTITION '5';
ALTER STREAM replicated_mutations_empty_partitions DROP PARTITION '9';

-- still ten records
SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/test/'||currentDatabase()||'/01586_replicated_mutations_empty_partitions/s1/block_numbers';

ALTER STREAM replicated_mutations_empty_partitions MODIFY COLUMN value uint64 SETTINGS replication_alter_partitions_sync=2;

SELECT sum(value) FROM replicated_mutations_empty_partitions;

SHOW create stream replicated_mutations_empty_partitions;

DROP STREAM IF EXISTS replicated_mutations_empty_partitions;
