-- Tags: distributed

SET query_mode = 'table';
drop stream if exists dist_01643;
drop stream if exists data_01643;

create stream data_01643 (key int) engine=Memory();

select 'no fsync';
create stream dist_01643 as data_01643 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01643, key);
system stop distributed sends dist_01643;
insert into dist_01643 select * from numbers(10) settings prefer_localhost_replica=0;
select sum(*) from dist_01643;
system flush distributed dist_01643;
select sum(*) from dist_01643;
drop stream dist_01643;

select 'fsync';
create stream dist_01643 as data_01643 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01643, key) settings fsync_after_insert=1, fsync_directories=1;
system stop distributed sends dist_01643;
insert into dist_01643 select * from numbers(10) settings prefer_localhost_replica=0;
select sum(*) from dist_01643;
system flush distributed dist_01643;
select sum(*) from dist_01643;
drop stream dist_01643;

drop stream if exists data_01643;
