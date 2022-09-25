-- Tags: distributed
SET query_mode = 'table';
drop stream if exists dist_01670;
drop stream if exists data_01670;

create stream data_01670 (key int) engine=Null();
create stream dist_01670 (key int) engine=Distributed(test_shard_localhost, currentDatabase(), data_01670) settings bytes_to_throw_insert=1;
system stop distributed sends dist_01670;
-- first batch is always OK, since there is no pending bytes yet
insert into dist_01670 select * from numbers(1) settings prefer_localhost_replica=0;
-- second will fail, because of bytes_to_throw_insert=1
-- (previous block definitelly takes more, since it has header)
insert into dist_01670 select * from numbers(1) settings prefer_localhost_replica=0; -- { serverError 574 }
system flush distributed dist_01670;
drop stream dist_01670;
drop stream data_01670;
