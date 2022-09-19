-- Tags: shard, no-parallel

SET query_mode = 'table';
create database if not exists shard_0;
create database if not exists shard_1;
drop stream if exists shard_0.tbl;
drop stream if exists shard_1.tbl;
drop stream if exists distr;

create stream shard_0.tbl (number uint64) engine = MergeTree order by number;
create stream shard_1.tbl (number uint64) engine = MergeTree order by number;
create stream distr (number uint64) engine = Distributed(test_cluster_two_shards_different_databases, '', tbl);

set insert_distributed_sync = 1;
set insert_distributed_one_random_shard = 1;
set max_block_size = 1;
set max_insert_block_size = 1;
set min_insert_block_size_rows = 1;
insert into distr select number from numbers(100);

select count() != 0 from shard_0.tbl;
select count() != 0 from shard_1.tbl;
select * from distr order by number LIMIT 20;

drop stream if exists shard_0.tbl;
drop stream if exists shard_1.tbl;
drop database shard_0;
drop database shard_1;
drop stream distr;
