-- Tags: shard

SET query_mode = 'table';
drop stream if exists tab;
create stream tab (val uint8) engine = MergeTree order by val;
insert into function remote('127.0.0.2', currentDatabase(), tab) values (1);
insert into function remote('127.0.0.{2|3}', currentDatabase(), tab) values (2);
insert into function remote('127.0.0.{2|3|4}', currentDatabase(), tab) values (3);
select * from tab order by val;
drop stream tab;
