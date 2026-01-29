drop view if exists 99108_mv;
drop view if exists 99108_mv2;
drop view if exists 99108_mv3;
drop view if exists 99108_mv4;
drop view if exists 99108_mv5;
drop view if exists 99108_mv6;
drop view if exists 99108_mv7;
drop stream if exists 99108_kv;
create stream 99108_kv(i int, v string) primary key i settings mode='versioned_kv',flush_threshold_count=1;

insert into 99108_kv(i, v) values(1, 'a1')(2, 'a2')(3, 'a3')(4, 'a4')(5, 'a5');

--- stream join hybrid hash table
create materialized view 99108_mv as select a.i, a.v, b.i, b.v from 99108_kv as a join table(99108_kv) as b on a.i = b.i settings default_hash_join='hybrid', join_algorithm='hash', max_hot_keys=2 STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99108_mv2 as select count() as cnt from 99108_kv as a join table(99108_kv) as b on a.i = b.i emit periodic 1s settings default_hash_join='hybrid', join_algorithm='hash', max_hot_keys=2 STORAGE_SETTINGS flush_threshold_count=1;
--- Concurrent stream join hybrid hash table
create materialized view 99108_mv3 as select a.i, a.v, b.i, b.v from 99108_kv as a join table(99108_kv) as b on a.i = b.i settings default_hash_join='hybrid', max_hot_keys=2, join_algorithm='parallel_hash', max_threads=8 STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99108_mv4 as select count() as cnt from 99108_kv as a join table(99108_kv) as b on a.i = b.i emit periodic 1s settings default_hash_join='hybrid', max_hot_keys=2, join_algorithm='parallel_hash', max_threads=8 STORAGE_SETTINGS flush_threshold_count=1;

--- stream cross join hybrid hash table (https://github.com/timeplus-io/proton-enterprise/issues/11420)
create materialized view 99108_mv5 as select a.i, a.v, b.i, b.v from 99108_kv as a join table(99108_kv) as b on 1=1 settings default_hash_join='hybrid' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99108_mv6 as select a.i, a.v, b.i, b.v from 99108_kv as a cross join table(99108_kv) as b settings default_hash_join='hybrid' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99108_mv7 as select a.i, a.v, b.i, b.v from 99108_kv as a, table(99108_kv) as b settings default_hash_join='hybrid' STORAGE_SETTINGS flush_threshold_count=1;

select sleep(2) format Null;

--- Validate checkpoint
system pause materialized view 99108_mv;
system pause materialized view 99108_mv2;
system pause materialized view 99108_mv3;
system pause materialized view 99108_mv4;
system pause materialized view 99108_mv5;
system pause materialized view 99108_mv6;
system pause materialized view 99108_mv7;
system resume materialized view 99108_mv;
system resume materialized view 99108_mv2;
system resume materialized view 99108_mv3;
system resume materialized view 99108_mv4;
system resume materialized view 99108_mv5;
system resume materialized view 99108_mv6;
system resume materialized view 99108_mv7;

select sleep(3) format Null;
insert into 99108_kv(i, v) values(1, 'aa1');

select sleep(3) format Null;
select '====== Stream join hybrid hash table ======';
select * except _tp_time from table(99108_mv) order by _tp_sn, i;
select '---';
select * except _tp_time from table(99108_mv2) order by _tp_sn;
select '====== Concurrent stream join hybrid hash table ======';
select * except _tp_time from table(99108_mv3) order by _tp_sn, i;
select '---';
select * except _tp_time from table(99108_mv4) order by _tp_sn;
select '====== Stream cross join hybrid hash table ======';
select * except _tp_time from table(99108_mv5) order by _tp_sn, i, v, b.i, b.v;
select '---';
select * except _tp_time from table(99108_mv6) order by _tp_sn, i, v, b.i, b.v;
select '---';
select * except _tp_time from table(99108_mv7) order by _tp_sn, i, v, b.i, b.v;

drop view if exists 99108_mv;
drop view if exists 99108_mv2;
drop view if exists 99108_mv3;
drop view if exists 99108_mv4;
drop view if exists 99108_mv5;
drop view if exists 99108_mv6;
drop view if exists 99108_mv7;
drop stream if exists 99108_kv;
