SET query_mode = 'table';
drop stream if exists data_01279;

create stream data_01279 (key string) Engine=TinyLog();
insert into data_01279 select reinterpret_as_string(number) from numbers(100000);

set max_rows_to_group_by=10;
set group_by_overflow_mode='any';
set group_by_two_level_threshold=100;
select * from data_01279 group by key format Null;

drop stream data_01279;
