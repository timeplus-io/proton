DROP STREAM IF EXISTS distributed_table_merged;
DROP STREAM IF EXISTS distributed_table_1;
DROP STREAM IF EXISTS distributed_table_2;
DROP STREAM IF EXISTS local_table_1;
DROP STREAM IF EXISTS local_table_2;
DROP STREAM IF EXISTS local_table_merged;

create stream local_table_1 (id string) ENGINE = MergeTree ORDER BY (id);
create stream local_table_2(id string) ENGINE = MergeTree ORDER BY (id);

create stream local_table_merged (id string) ENGINE = Merge('default', 'local_table_1|local_table_2');

create stream distributed_table_1 (id string) ENGINE = Distributed(test_shard_localhost, default, local_table_1);
create stream distributed_table_2 (id string) ENGINE = Distributed(test_shard_localhost, default, local_table_2);

create stream distributed_table_merged (id string) ENGINE = Merge('default', 'distributed_table_1|distributed_table_2');

SELECT 1 FROM distributed_table_merged;

DROP STREAM IF EXISTS distributed_table_merged;
DROP STREAM IF EXISTS distributed_table_1;
DROP STREAM IF EXISTS distributed_table_2;
DROP STREAM IF EXISTS local_table_1;
DROP STREAM IF EXISTS local_table_2;
DROP STREAM local_table_merged;
