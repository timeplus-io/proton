-- Tags: shard
SET query_mode = 'table';
set optimize_skip_unused_shards=1;

drop stream if exists data_01755;
drop stream if exists dist_01755;

create stream data_01755 (i int) Engine=Memory;
create stream dist_01755 as data_01755 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01755, i);

insert into data_01755 values (1);

select * from dist_01755 where 1 settings enable_early_constant_folding = 0;

drop stream if exists data_01755;
drop stream if exists dist_01755;
