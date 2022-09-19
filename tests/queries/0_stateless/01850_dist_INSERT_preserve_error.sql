-- Tags: no-parallel
SET query_mode = 'table';
create database if not exists shard_0;
create database if not exists shard_1;

drop stream if exists dist_01850;
drop stream if exists shard_0.data_01850;

create stream shard_0.data_01850 (key int) engine=Memory();
create stream dist_01850 (key int) engine=Distributed('test_cluster_two_replicas_different_databases', /* default_database= */ '', data_01850, key);

set insert_distributed_sync=1;
set prefer_localhost_replica=0;
insert into dist_01850 values (1); -- { serverError 60 }

drop stream if exists dist_01850;
drop stream shard_0.data_01850;

drop database shard_0;
drop database shard_1;
