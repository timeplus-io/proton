drop view if exists 99060_mv1;
drop view if exists 99060_mv2;
drop view if exists 99060_mv3;
drop view if exists 99060_mv4;
drop view if exists 99060_mv5;
drop view if exists 99060_mv6;
drop view if exists 99060_mv11;
drop view if exists 99060_mv12;
drop view if exists 99060_mv13;
drop view if exists 99060_mv14;
drop view if exists 99060_mv15;
drop view if exists 99060_mv16;
drop stream if exists 99060_stream;

create stream 99060_stream(i int32, ii int64);
select sleep(1) format Null;

create materialized view 99060_mv1 as select i, ii, count() from 99060_stream group by i, ii emit settings default_hash_table='memory';
create materialized view 99060_mv2 as select i, ii, count() from 99060_stream group by ii, i emit settings default_hash_table='memory';
create materialized view 99060_mv3 as select i, ii, count() from 99060_stream group by i, ii emit on update settings default_hash_table='memory';
create materialized view 99060_mv4 as select i, ii, count() from 99060_stream group by ii, i emit on update settings default_hash_table='memory';
create materialized view 99060_mv5 as select i, ii, count(), _tp_delta from 99060_stream group by i, ii emit changelog settings default_hash_table='memory';
create materialized view 99060_mv6 as select i, ii, count(), _tp_delta from 99060_stream group by ii, i emit changelog settings default_hash_table='memory';
create materialized view 99060_mv11 as select i, ii, count() from 99060_stream group by i, ii emit settings default_hash_table='hybrid';
create materialized view 99060_mv12 as select i, ii, count() from 99060_stream group by ii, i emit settings default_hash_table='hybrid';
create materialized view 99060_mv13 as select i, ii, count() from 99060_stream group by i, ii emit on update settings default_hash_table='hybrid';
create materialized view 99060_mv14 as select i, ii, count() from 99060_stream group by ii, i emit on update settings default_hash_table='hybrid';
create materialized view 99060_mv15 as select i, ii, count(), _tp_delta from 99060_stream group by i, ii emit changelog settings default_hash_table='hybrid';
create materialized view 99060_mv16 as select i, ii, count(), _tp_delta from 99060_stream group by ii, i emit changelog settings default_hash_table='hybrid';

select sleep(1) format Null;
insert into 99060_stream(i, ii) values(1, 1)(1, 2)(2, 2);
select sleep(3) format Null;

select '===== memory =====';
select * except _tp_time from table(99060_mv1) order by i, ii;
select * except _tp_time from table(99060_mv2) order by i, ii;
select * except _tp_time from table(99060_mv3) order by i, ii;
select * except _tp_time from table(99060_mv4) order by i, ii;
select * except _tp_time from table(99060_mv5) order by i, ii;
select * except _tp_time from table(99060_mv6) order by i, ii;

select '===== hybrid =====';
select * except _tp_time from table(99060_mv11) order by i, ii;
select * except _tp_time from table(99060_mv12) order by i, ii;
select * except _tp_time from table(99060_mv13) order by i, ii;
select * except _tp_time from table(99060_mv14) order by i, ii;
select * except _tp_time from table(99060_mv15) order by i, ii;
select * except _tp_time from table(99060_mv16) order by i, ii;

drop view 99060_mv1;
drop view 99060_mv2;
drop view 99060_mv3;
drop view 99060_mv4;
drop view 99060_mv5;
drop view 99060_mv6;
drop view 99060_mv11;
drop view 99060_mv12;
drop view 99060_mv13;
drop view 99060_mv14;
drop view 99060_mv15;
drop view 99060_mv16;
drop stream 99060_stream;
