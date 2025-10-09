drop view if exists 99090_mv;
drop view if exists 99090_mv2;
drop view if exists 99090_mv3;
drop view if exists 99090_mv4;
drop stream if exists 99090_left_stream;
drop stream if exists 99090_right_stream;
create stream 99090_left_stream(i int, v string) settings flush_threshold_count=1;
create stream 99090_right_stream(i int, v string) settings flush_threshold_count=1;

--- Concurrent hash join
create materialized view 99090_mv as select a.i, a.v, b.i, b.v from 99090_left_stream as a join 99090_right_stream as b on a.i = b.i settings join_algorithm = 'parallel_hash', max_threads=8 STORAGE_SETTINGS flush_threshold_count=1;
--- Concurrent hybrid hash join
create materialized view 99090_mv2 as select a.i, a.v, b.i, b.v from 99090_left_stream as a join 99090_right_stream as b on a.i = b.i settings join_algorithm = 'parallel_hash', max_threads=8, default_hash_join='hybrid', max_hot_keys=1 STORAGE_SETTINGS flush_threshold_count=1;
--- Concurrent hash join with alignment
create materialized view 99090_mv3 as select a.i, a.v, b.i, b.v from 99090_left_stream as a join 99090_right_stream as b on a.i = b.i and lag_behind(1ms, a._tp_time, b._tp_time) settings join_algorithm = 'parallel_hash', max_threads=8 STORAGE_SETTINGS flush_threshold_count=1;
--- Concurrent hybrid hash join with alignment
create materialized view 99090_mv4 as select a.i, a.v, b.i, b.v from 99090_left_stream as a join 99090_right_stream as b on a.i = b.i and lag_behind(1ms, a._tp_time, b._tp_time) settings join_algorithm = 'parallel_hash', max_threads=8, default_hash_join='hybrid', max_hot_keys=1 STORAGE_SETTINGS flush_threshold_count=1;

select sleep(2) format Null;
insert into 99090_left_stream(i, v) values(1, 'a1');
insert into 99090_right_stream(i, v) values(1, 'b1');

--- Validate checkpoint
system pause materialized view 99090_mv;
system pause materialized view 99090_mv2;
system pause materialized view 99090_mv3;
system pause materialized view 99090_mv4;
system resume materialized view 99090_mv;
system resume materialized view 99090_mv2;
system resume materialized view 99090_mv3;
system resume materialized view 99090_mv4;

insert into 99090_right_stream(i, v) values(1, 'b2');
insert into 99090_right_stream(i, v) values(2, 'b1');
insert into 99090_left_stream(i, v) values(2, 'a1');
insert into 99090_left_stream(i, v) values(1, 'a2');

select sleep(3) format Null;
select '====== Concurrent hash join ======';
select * except _tp_time from table(99090_mv) order by i, v, b.i, b.v;
select '====== Concurrent hybrid hash join ======';
select * except _tp_time from table(99090_mv2) order by i, v, b.i, b.v;
select '====== Concurrent hash join with alignment ======';
select * except _tp_time from table(99090_mv3) order by i, v, b.i, b.v;
select '====== Concurrent hybrid hash join with alignment ======';
select * except _tp_time from table(99090_mv4) order by i, v, b.i, b.v;

drop view if exists 99090_mv;
drop view if exists 99090_mv2;
drop view if exists 99090_mv3;
drop view if exists 99090_mv4;
drop stream if exists 99090_left_stream;
drop stream if exists 99090_right_stream;
