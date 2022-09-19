SET query_mode = 'table';
drop stream if exists lc_00906;
create stream lc_00906 (b LowCardinality(string)) engine=MergeTree order by b;
insert into lc_00906 select '0123456789' from numbers(100000000);
select count(), b from lc_00906 group by b;
drop stream if exists lc_00906;
