-- Tags: shard, no-parallel

set allow_nondeterministic_optimize_skip_unused_shards=1;
set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=2;
set check_table_dependencies=0;
SET query_mode = 'table';

drop database if exists db_01527_ranges;
drop stream if exists dist_01527;
drop stream if exists data_01527;

create database db_01527_ranges;

create stream data_01527 engine=Memory() as select to_uint64(number) key from numbers(2);
create stream dist_01527 as data_01527 engine=Distributed('test_cluster_two_shards', currentDatabase(), data_01527, dictGetUInt64('db_01527_ranges.dict', 'shard', key));

create stream db_01527_ranges.data engine=Memory() as select number key, number shard from numbers(100);
create dictionary db_01527_ranges.dict (key uint64, shard uint64) primary key key source(clickhouse(host '127.0.0.1' port tcpPort() table 'data' db 'db_01527_ranges' user 'default' password '')) lifetime(0) layout(hashed());
system reload dictionary db_01527_ranges.dict;

select _shard_num from dist_01527 where key=0;
select _shard_num from dist_01527 where key=1;

drop stream db_01527_ranges.data sync;
create stream db_01527_ranges.data engine=Memory() as select number key, number+1 shard from numbers(100);
system reload dictionary db_01527_ranges.dict;

select _shard_num from dist_01527 where key=0;
select _shard_num from dist_01527 where key=1;

drop stream data_01527;
drop stream dist_01527;
drop stream db_01527_ranges.data;
drop dictionary db_01527_ranges.dict;
drop database db_01527_ranges;
