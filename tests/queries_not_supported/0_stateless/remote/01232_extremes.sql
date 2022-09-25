-- Tags: no-parallel

set send_logs_level = 'error';
set extremes = 1;

select * from remote('127.0.0.1', numbers(2));
select '-';
select * from remote('127.0.0.{1,1}', numbers(2));
select '-';
select * from remote('127.0.0.{1,2}', numbers(2));
select '-';
select * from remote('127.0.0.{2,2}', numbers(2));
select '-';
select * from remote('127.0.0.2', numbers(2));
select '------';

select * from (select * from numbers(2) union all select * from numbers(3) union all select * from numbers(1)) order by number;
select '-';
select * from (select * from numbers(1) union all select * from numbers(2) union all select * from numbers(3)) order by number;
select '-';
select * from (select * from numbers(3) union all select * from numbers(1) union all select * from numbers(2)) order by number;
select '------';

create database if not exists shard_0;
create database if not exists shard_1;
SET query_mode = 'table';
drop stream if exists shard_0.num_01232;
drop stream if exists shard_0.num2_01232;
drop stream if exists shard_1.num_01232;
drop stream if exists shard_1.num2_01232;
drop stream if exists distr;
drop stream if exists distr2;

create stream shard_0.num_01232 (number uint64) engine = MergeTree order by number;
create stream shard_1.num_01232 (number uint64) engine = MergeTree order by number;
insert into shard_0.num_01232 select number from numbers(2);
insert into shard_1.num_01232 select number from numbers(3);
create stream distr (number uint64) engine = Distributed(test_cluster_two_shards_different_databases, '', num_01232);

create stream shard_0.num2_01232 (number uint64) engine = MergeTree order by number;
create stream shard_1.num2_01232 (number uint64) engine = MergeTree order by number;
insert into shard_0.num2_01232 select number from numbers(3);
insert into shard_1.num2_01232 select number from numbers(2);
create stream distr2 (number uint64) engine = Distributed(test_cluster_two_shards_different_databases, '', num2_01232);

select * from distr order by number;
select '-';
select * from distr2 order by number;

drop stream if exists shard_0.num_01232;
drop stream if exists shard_0.num2_01232;
drop stream if exists shard_1.num_01232;
drop stream if exists shard_1.num2_01232;
drop stream if exists distr;
drop stream if exists distr2;

drop database shard_0;
drop database shard_1;
