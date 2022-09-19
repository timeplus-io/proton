SET query_mode = 'table';
drop stream if exists my_table;
drop view if exists my_view;
create stream my_table(Id uint32, Object nested(Key uint8, Value string)) engine MergeTree order by Id;
create view my_view as select * replace array_map(x -> x + 1,`Object.Key`) as `Object.Key` from my_table;

show create my_view;

drop stream my_table;
drop view my_view;
