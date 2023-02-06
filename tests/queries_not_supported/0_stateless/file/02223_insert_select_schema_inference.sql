drop stream if exists test;
create stream test (x uint32, y string, d Date) engine=Memory() as select number as x, to_string(number) as y, to_date(number) as d from numbers(10);
insert into stream function file('data.native.zst') select * from test settings engine_file_truncate_on_insert=1;
desc file('data.native.zst');
select * from file('data.native.zst');
