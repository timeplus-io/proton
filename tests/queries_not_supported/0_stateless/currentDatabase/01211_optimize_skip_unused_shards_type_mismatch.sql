-- Tags: shard

set optimize_skip_unused_shards=1;
SET query_mode = 'table';
drop stream if exists data_02000;
drop stream if exists dist_02000;

create stream data_02000 (key int) Engine=Null();
create stream dist_02000 as data_02000 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_02000, key);

select * from data_02000 where key = 0xdeadbeafdeadbeaf;
select * from dist_02000 where key = 0xdeadbeafdeadbeaf settings force_optimize_skip_unused_shards=2; -- { serverError 507; }
select * from dist_02000 where key = 0xdeadbeafdeadbeaf;

drop stream data_02000;
drop stream dist_02000;
