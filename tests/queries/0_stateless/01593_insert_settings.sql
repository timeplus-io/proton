SET query_mode = 'table';
drop stream if exists data_01593;
create stream data_01593 (key int) engine=MergeTree() order by key partition by key;

insert into data_01593 select * from numbers_mt(10);
-- TOO_MANY_PARTS error
insert into data_01593 select * from numbers_mt(10) settings max_partitions_per_insert_block=1; -- { serverError 252 }
-- settings for INSERT is prefered
insert into data_01593 select * from numbers_mt(10) settings max_partitions_per_insert_block=1 settings max_partitions_per_insert_block=100;

drop stream data_01593;
