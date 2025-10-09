drop view if exists 99102_mv;
drop view if exists 99102_mv2;
drop stream if exists 99102_stream;

create stream 99102_stream(i int) settings flush_threshold_count=1;
select sleep(2) format Null;
insert into 99102_stream(i) values (1);
insert into 99102_stream(i) values (2);
select sleep(2) format Null;

--- Execute historical pipelines sequentially (By Default)
create materialized view 99102_mv as
select 'table1' as source, 'k1' as key, i as value from table(99102_stream)
union
select 'table2' as source, 'k2' as key, i as value from table(99102_stream)
union
select 'stream3' as source, 'k3' as key, i as value from 99102_stream
settings union_historical_queries_in_parallel=false
STORAGE_SETTINGS flush_threshold_count=1;

--- Execute historical pipelines in parallel
create materialized view 99102_mv2 as
select 'table1' as source, 'k1' as key, i as value from table(99102_stream)
union
select 'table2' as source, 'k2' as key, i as value from table(99102_stream)
union
select 'stream3' as source, 'k3' as key, i as value from 99102_stream
settings union_historical_queries_in_parallel=true
STORAGE_SETTINGS flush_threshold_count=1;

select sleep(2) format Null;
insert into 99102_stream(i) values (3);

select sleep(2) format Null;
select '======== Execute historical pipelines sequentially ========';
select source, key from table(99102_mv) order by _tp_sn;
select '======== Execute historical pipelines in parallel  ========';
select substr(source, 1, length(source) - 1) as source_prefix from table(99102_mv2) order by _tp_sn;

drop view if exists 99102_mv;
drop view if exists 99102_mv2;
drop stream if exists 99102_stream;
