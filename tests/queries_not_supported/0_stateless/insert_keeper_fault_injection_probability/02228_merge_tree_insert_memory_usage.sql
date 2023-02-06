-- Tags: long, no-parallel
SET insert_keeper_fault_injection_probability=0; -- to succeed this test can require too many retries due to 1024 partitions, so disable fault injections

-- regression for MEMORY_LIMIT_EXCEEDED error because of deferred final part flush

drop stream if exists data_02228;
create stream data_02228 (key1 uint32, sign int8, s uint64) engine = CollapsingMergeTree(sign) order by (key1) partition by key1 % 1024;
insert into data_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=0;
insert into data_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=10000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
drop stream data_02228;

drop stream if exists data_rep_02228 SYNC;
create stream data_rep_02228 (key1 uint32, sign int8, s uint64) engine = ReplicatedCollapsingMergeTree('/clickhouse/{database}', 'r1', sign) order by (key1) partition by key1 % 1024;
insert into data_rep_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=0;
insert into data_rep_02228 select number, 1, number from numbers_mt(100e3) settings max_memory_usage='300Mi', max_partitions_per_insert_block=1024, max_insert_delayed_streams_for_parallel_write=10000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
drop stream data_rep_02228 SYNC;
