SET query_mode = 'table';
drop stream if exists data_02021;
create stream data_02021 (key int) engine=MergeTree() order by key;
insert into data_02021 values (1);
select count() from data_02021 prewhere 1 or ignore(key) where ignore(key)=0;
drop stream data_02021;
