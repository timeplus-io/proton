drop stream if exists test_table;

create stream test_table (A nullable(string), B nullable(string)) engine MergeTree order by (A,B) settings index_granularity = 1, allow_nullable_key=1;

insert into test_table values ('a', 'b'), ('a', null), (null, 'b');

select * from test_table where B is null;

drop stream test_table;
