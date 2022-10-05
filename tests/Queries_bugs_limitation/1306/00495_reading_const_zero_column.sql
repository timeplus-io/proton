SET query_mode = 'table';
drop stream if exists one_table;
create stream one_table (date date, one uint64) engine = MergeTree(date, (date, one), 8192);
insert into one_table select today(), to_uint64(1) from system.numbers limit 100000;
SET preferred_block_size_bytes = 8192;
select is_null(one) from one_table where is_null(one);
drop stream if exists one_table;
