SET query_mode = 'table';
drop stream if exists `table_00653`;
create stream `table_00653` (val int32) engine = MergeTree order by val;
insert into `table_00653` values (-2), (0), (2);
select count() from `table_00653` where to_uint64(val) == 0;
drop stream table_00653;
