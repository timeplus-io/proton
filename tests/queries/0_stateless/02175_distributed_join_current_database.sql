-- Tags: shard
SET query_mode = 'table';
drop stream if exists local_02175;
drop stream if exists dist_02175;

create stream local_02175 engine=Memory() as select * from system.one;
create stream dist_02175 as local_02175 engine=Distributed(test_cluster_two_shards, currentDatabase(), local_02175);

-- { echoOn }
select * from dist_02175 l join local_02175 r using dummy;
select * from dist_02175 l global join local_02175 r using dummy;

-- explicit database for distributed table
select * from remote('127.1', currentDatabase(), dist_02175) l join local_02175 r using dummy;
select * from remote('127.1', currentDatabase(), dist_02175) l global join local_02175 r using dummy;

-- { echoOff }
drop stream local_02175;
drop stream dist_02175;
