SET query_mode = 'table';
drop stream if exists insub;

create stream insub (i int, j int) engine MergeTree order by i settings index_granularity = 1;
insert into insub select number a, a + 2 from numbers(10);

SET max_rows_to_read = 12; -- 10 from numbers + 2 from table
select * from insub where i in (select to_int32(3) from numbers(10));

drop stream if exists insub;
