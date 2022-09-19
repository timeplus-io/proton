SET query_mode = 'table';
drop stream if exists data_02021;
create stream data_02021 (key int) engine=MergeTree() order by key;
insert into data_02021 values (1);
-- { echoOn }
select * from data_02021 prewhere 1 or ignore(key);
select * from data_02021 prewhere 1 or ignore(key) where key = 1;
select * from data_02021 prewhere 0 or ignore(key);
select * from data_02021 prewhere 0 or ignore(key) where key = 1;
-- { echoOff }
drop stream data_02021;
