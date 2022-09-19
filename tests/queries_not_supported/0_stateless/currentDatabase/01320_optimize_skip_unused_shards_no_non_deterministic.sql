-- Tags: shard
SET query_mode = 'table';
drop stream if exists data_01320;
drop stream if exists dist_01320;

create stream data_01320 (key int) Engine=Null();
-- non deterministic function (i.e. rand())
create stream dist_01320 as data_01320 Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01320, key + rand());

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;
select * from dist_01320 where key = 0; -- { serverError 507 }

drop stream data_01320;
drop stream dist_01320;
