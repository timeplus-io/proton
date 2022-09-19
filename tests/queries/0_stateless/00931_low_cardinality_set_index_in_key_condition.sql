SET query_mode = 'table';
drop stream if exists test_in;
create stream test_in (a LowCardinality(string)) Engine = MergeTree order by a;

insert into test_in values ('a');
select * from test_in where a in ('a');

drop stream if exists test_in;
