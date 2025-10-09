drop view if exists 99053_mv1;
drop view if exists 99053_mv2;
drop view if exists 99053_mv3;
drop view if exists 99053_mv4;
drop view if exists 99053_mv5;
drop view if exists 99053_mv6;
drop view if exists 99053_v6;
drop stream if exists 99053_stream;

create stream 99053_stream(id uint64) comment 'Source stream for testing';
create materialized view 99053_mv1 as select sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) settings default_hash_table='memory';
create materialized view 99053_mv2 as select i, sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) group by i settings default_hash_table='memory';
create materialized view 99053_mv3 as select sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) settings default_hash_table='hybrid';
create materialized view 99053_mv4 as select i, sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) group by i settings default_hash_table='hybrid';
--- three level global aggr (covering cte subquery and view)
create materialized view 99053_mv5 as with cte as (select i, sum(cnt) as cnt from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) group by i) select sum(cnt) from cte settings default_hash_table='memory';
create view 99053_v6 as (with cte as (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) select i, sum(cnt) as cnt from cte group by i);
create materialized view 99053_mv6 as select sum(cnt) from 99053_v6 settings default_hash_table='hybrid';

select sleep(2) format Null;
select sleep(2) format Null;
insert into 99053_stream(id) values(1);
select sleep(1) format Null;
insert into 99053_stream(id) values(1);
select sleep(1) format Null;
insert into 99053_stream(id) values(1);
select sleep(1) format Null;
insert into 99053_stream(id) values(1);

select sleep(2) format Null;
select sleep(2) format Null;
select * except _tp_time from table(99053_mv1) order by _tp_time;
select * except _tp_time from table(99053_mv2) order by _tp_time;
select * except _tp_time from table(99053_mv3) order by _tp_time;
select * except _tp_time from table(99053_mv4) order by _tp_time;
select * except _tp_time from table(99053_mv5) order by _tp_time;
select * except _tp_time from table(99053_mv6) order by _tp_time;
select sleep(2) format Null;

drop view 99053_mv1;
drop view 99053_mv2;
drop view 99053_mv3;
drop view 99053_mv4;
drop view 99053_mv5;
drop view 99053_mv6;
drop view 99053_v6;
drop stream 99053_stream;
