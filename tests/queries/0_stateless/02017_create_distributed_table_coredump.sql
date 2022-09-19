-- Tags: distributed

SET query_mode = 'table';
drop stream if exists t;
drop stream if exists td1;
drop stream if exists td2;
drop stream if exists td3;
create stream t (val uint32) engine = MergeTree order by val;
create stream td1 engine = Distributed(test_shard_localhost, currentDatabase(), 't') as t;
create stream td2 engine = Distributed(test_shard_localhost, currentDatabase(), 't', xxHash32(val), default) as t;
create stream td3 engine = Distributed(test_shard_localhost, currentDatabase(), 't', xxHash32(val), 'default') as t;
drop stream if exists t;
drop stream if exists td1; 
drop stream if exists td2; 
drop stream if exists td3; 
