-- Tags: distributed
SET query_mode = 'table';
drop stream if exists tab;
create stream tab (date date,  time DateTime, data string) ENGINE = MergeTree(date, (time, data), 8192);
insert into tab values ('2018-01-21','2018-01-21 15:12:13','test');
select time FROM remote('127.0.0.{1,2}', currentDatabase(), tab)  WHERE date = '2018-01-21' limit 2;
drop stream tab;
