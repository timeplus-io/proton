SET query_mode = 'table';
drop stream if exists `table_00609`;
create stream `table_00609` (key uint64, val uint64) engine = MergeTree order by key settings index_granularity=8192;
insert into `table_00609` select number, number / 8192 from system.numbers limit 100000; 
alter stream `table_00609` add column def uint64 default val + 1;
select * from `table_00609` prewhere val > 2 format Null;

drop stream if exists `table_00609`;
create stream `table_00609` (key uint64, val uint64) engine = MergeTree order by key settings index_granularity=8192;
insert into `table_00609` select number, number / 8192 from system.numbers limit 100000; 
alter stream `table_00609` add column def uint64;
select * from `table_00609` prewhere val > 2 format Null;

drop stream if exists `table_00609`;
