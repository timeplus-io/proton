drop view if exists 99053_mv1;
drop view if exists 99053_mv2;
drop view if exists 99053_mv3;
drop view if exists 99053_mv4;
drop view if exists 99053_mv5;
drop view if exists 99053_mv6;
drop view if exists 99053_mv7;
drop view if exists 99053_mv8;
drop view if exists 99053_v6;
drop stream if exists 99053_stream;
drop stream if exists 99053_stream7_left;
drop stream if exists 99053_stream7_right;
drop stream if exists 99053_stream8_left;
drop stream if exists 99053_stream8_right;

create stream 99053_stream(id uint64) comment 'Source stream for testing';
create materialized view 99053_mv1 as select sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) settings default_hash_table='memory';
create materialized view 99053_mv2 as select i, sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) group by i settings default_hash_table='memory';
create materialized view 99053_mv3 as select sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) settings default_hash_table='hybrid';
create materialized view 99053_mv4 as select i, sum(cnt) from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) group by i settings default_hash_table='hybrid';
--- three level global aggr (covering cte subquery and view)
create materialized view 99053_mv5 as with cte as (select i, sum(cnt) as cnt from (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) group by i) select sum(cnt) from cte settings default_hash_table='memory';
create view 99053_v6 as (with cte as (select 1 as i, count() as cnt from 99053_stream group by i emit periodic 1ms) select i, sum(cnt) as cnt from cte group by i);
create materialized view 99053_mv6 as select sum(cnt) from 99053_v6 settings default_hash_table='hybrid';
--- nested global aggr over append-only stream join table(stream)
create stream 99053_stream7_left(id uint64, agg_key uint64);
create stream 99053_stream7_right(id uint64, val uint64);
create materialized view 99053_mv7 as
select agg_key, sum(cnt)
from (
    select s.agg_key as agg_key, count() as cnt
    from 99053_stream7_left as s
    inner join table(99053_stream7_right) as t on s.id = t.id
    group by s.agg_key
)
group by agg_key;

--- aggr over append-only stream join table(stream) with emit changelog (force_emit_changelog)
create stream 99053_stream8_left(id uint64, val uint64);
create stream 99053_stream8_right(id uint64, name string);
insert into 99053_stream8_right(id, name) values(1, 'a');
select sleep(1) format Null;
create materialized view 99053_mv8 as
select count() as cnt, _tp_delta
from 99053_stream8_left as s
inner join table(99053_stream8_right) as t on s.id = t.id
emit changelog periodic 1ms;

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
--- mv8: insert data and verify aggr + join + emit changelog
insert into 99053_stream8_left(id, val) values(1, 10);
select sleep(1) format Null;
insert into 99053_stream8_left(id, val) values(1, 20);
select sleep(2) format Null;
select sleep(2) format Null;
select cnt, _tp_delta from table(99053_mv8) order by _tp_sn;
select sleep(2) format Null;

drop view 99053_mv1;
drop view 99053_mv2;
drop view 99053_mv3;
drop view 99053_mv4;
drop view 99053_mv5;
drop view 99053_mv6;
drop view 99053_mv7;
drop view 99053_mv8;
drop view 99053_v6;
drop stream 99053_stream;
drop stream 99053_stream7_left;
drop stream 99053_stream7_right;
drop stream 99053_stream8_left;
drop stream 99053_stream8_right;
