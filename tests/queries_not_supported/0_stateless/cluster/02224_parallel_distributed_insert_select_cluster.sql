drop stream if exists dst_02224;
drop stream if exists src_02224;
create stream dst_02224 (key int) engine=Memory();
create stream src_02224 (key int) engine=Memory();
insert into src_02224 values (1);

-- { echoOn }
truncate stream dst_02224;
insert into function cluster('test_cluster_two_shards', current_database(), dst_02224, key)
select * from cluster('test_cluster_two_shards', current_database(), src_02224, key)
settings parallel_distributed_insert_select=1, max_distributed_depth=1; -- { serverError TOO_LARGE_DISTRIBUTED_DEPTH }
select * from dst_02224;

truncate stream dst_02224;
insert into function cluster('test_cluster_two_shards', current_database(), dst_02224, key)
select * from cluster('test_cluster_two_shards', current_database(), src_02224, key)
settings parallel_distributed_insert_select=1, max_distributed_depth=2;
select * from dst_02224;

truncate stream dst_02224;
insert into function cluster('test_cluster_two_shards', current_database(), dst_02224, key)
select * from cluster('test_cluster_two_shards', current_database(), src_02224, key)
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02224;

truncate stream dst_02224;
insert into function remote('127.{1,2}', current_database(), dst_02224, key)
select * from remote('127.{1,2}', current_database(), src_02224, key)
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02224;
-- { echoOff }

drop stream src_02224;
drop stream dst_02224;
