SET query_mode = 'table';
drop stream if exists src_table_1;
drop stream if exists src_table_2;
drop stream if exists src_table_3;
drop stream if exists set;

create stream src_table_1 (n uint64) engine=Memory as select * from numbers(10);
create stream src_table_2 (n uint64) engine=Log as select number * 10 from numbers(10);
create stream src_table_3 (n uint64) engine=MergeTree order by n as select number * 100 from numbers(10);
create stream set (s string) engine=Set as select array_join(['src_table_1', 'src_table_2']);

create temporary table tmp (s string);
insert into tmp values ('src_table_1'), ('src_table_3');

select count(), sum(n) from merge(currentDatabase(), 'src_table');
-- FIXME #21401 select count(), sum(n) from merge(currentDatabase(), 'src_table') where _table = 'src_table_1' or to_int8(substr(_table, 11, 1)) = 2;
select count(), sum(n) from merge(currentDatabase(), 'src_table') where _table in ('src_table_2', 'src_table_3');
select count(), sum(n) from merge(currentDatabase(), 'src_table') where _table in ('src_table_2', 'src_table_3') and n % 20 = 0;
select count(), sum(n) from merge(currentDatabase(), 'src_table') where _table in set;
select count(), sum(n) from merge(currentDatabase(), 'src_table') where _table in tmp;
select count(), sum(n) from merge(currentDatabase(), 'src_table') where _table in set and n % 2 = 0;
select count(), sum(n) from merge(currentDatabase(), 'src_table') where n % 2 = 0 and _table in tmp;

drop stream src_table_1;
drop stream src_table_2;
drop stream src_table_3;
drop stream set;
