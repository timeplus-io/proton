drop stream if exists data_01801;
create stream data_01801 (key int) engine=MergeTree() order by key settings index_granularity=10 as select number/10 from numbers(100);

select * from data_01801 where key = 0 order by key settings max_rows_to_read=9 format Null; -- { serverError 158 }
select * from data_01801 where key = 0 order by key desc settings max_rows_to_read=9 format Null; -- { serverError 158 }

select * from data_01801 where key = 0 order by key settings max_rows_to_read=10 format Null;
select * from data_01801 where key = 0 order by key desc settings max_rows_to_read=10 format Null;

drop stream data_01801;
