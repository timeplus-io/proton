SET query_mode = 'table';
drop stream if exists lc_00800_1;
create stream lc_00800_1 (names array(LowCardinality(string))) engine=MergeTree order by tuple();
insert into lc_00800_1 values ([]);
insert into lc_00800_1 select empty_array_string();
select * from lc_00800_1;
drop stream if exists lc_00800_1;

