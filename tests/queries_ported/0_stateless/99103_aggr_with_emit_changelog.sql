select '=== Single shard aggr with emit changelog results ===';
drop view if exists 99103_mv;
drop view if exists 99103_mv2;
drop view if exists 99103_mv3;
drop view if exists 99103_mv4;
drop stream if exists 99103_stream;
create stream 99103_stream(i int) settings flush_threshold_count=1;
create materialized view 99103_mv as select count(), _tp_delta from 99103_stream emit changelog periodic 3s STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99103_mv2 as select i, count(), _tp_delta from 99103_stream group by i emit changelog periodic 3s STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99103_mv3 as select count(), _tp_delta from 99103_stream emit changelog periodic 3s settings default_hash_table='hybrid',max_hot_keys=2 STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99103_mv4 as select i, count(), _tp_delta from 99103_stream group by i emit changelog periodic 3s settings default_hash_table='hybrid',max_hot_keys=2 STORAGE_SETTINGS flush_threshold_count=1;

select sleep(1) format Null;
insert into 99103_stream(i) values(1);
select sleep(3) format Null;
insert into 99103_stream(i) values(2);
insert into 99103_stream(i) values(2);

-- Validate checkpoint
system pause materialized view 99103_mv;
system pause materialized view 99103_mv2;
system pause materialized view 99103_mv3;
system pause materialized view 99103_mv4;
system resume materialized view 99103_mv;
system resume materialized view 99103_mv2;
system resume materialized view 99103_mv3;
system resume materialized view 99103_mv4;
select sleep(1) format Null;
select sleep(3) format Null;
insert into 99103_stream(i) values(2);
insert into 99103_stream(i) values(3);
select sleep(3) format Null;
select sleep(1) format Null;

select '>>> Pure memory aggr without key';
select * except _tp_time from table(99103_mv) order by _tp_sn;
select '>>> Pure memory aggr';
select * except _tp_time from table(99103_mv2) order by _tp_sn, i; --- bug issue: https://github.com/timeplus-io/proton-enterprise/issues/9532
select '>>> Hybrid aggr without key';
select * except _tp_time from table(99103_mv3) order by _tp_sn;
select '>>> Hybrid aggr';
select * except _tp_time from table(99103_mv4) order by _tp_sn, i;

select '';
select '=== Multi-shard aggr with emit changelog results ===';
drop view if exists 99103_mv;
drop view if exists 99103_mv2;
drop view if exists 99103_mv3;
drop view if exists 99103_mv4;
drop stream if exists 99103_stream;
create stream 99103_stream(i int, k string) settings shards=3, flush_threshold_count=1;
create materialized view 99103_mv as select min(i), max(i), count(), _tp_delta from 99103_stream emit changelog periodic 3s STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99103_mv2 as select i, min(k), max(k), count(), _tp_delta from 99103_stream group by i emit changelog periodic 3s STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99103_mv3 as select min(i), max(i), count(), _tp_delta from 99103_stream emit changelog periodic 3s settings default_hash_table='hybrid',max_hot_keys=2 STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99103_mv4 as select i, min(k), max(k), count(), _tp_delta from 99103_stream group by i emit changelog periodic 3s settings default_hash_table='hybrid',max_hot_keys=2 STORAGE_SETTINGS flush_threshold_count=1;

select sleep(1) format Null;
insert into 99103_stream(i, k) values (1, 'k1');
select sleep(3) format Null;
insert into 99103_stream(i, k) values (1, 'k2');
-- Validate checkpoint
system pause materialized view 99103_mv;
system pause materialized view 99103_mv2;
system pause materialized view 99103_mv3;
system pause materialized view 99103_mv4;
system resume materialized view 99103_mv;
system resume materialized view 99103_mv2;
system resume materialized view 99103_mv3;
system resume materialized view 99103_mv4;
select sleep(1) format Null;
select sleep(3) format Null;
insert into 99103_stream(i, k) values (1, 'k3')(2, 't1')(2, 't2');
select sleep(3) format Null;
select sleep(1) format Null;

select '>>> Pure memory aggr without key:';
select * except _tp_time from table(99103_mv) order by _tp_sn;
select '>>> Pure memory aggr:';
select * except _tp_time from table(99103_mv2) order by _tp_sn, i;
select '>>> Hybrid aggr without key:';
select * except _tp_time from table(99103_mv3) order by _tp_sn;
select '>>> Hybrid aggr:';
select * except _tp_time from table(99103_mv4) order by _tp_sn, i;

drop view if exists 99103_mv;
drop view if exists 99103_mv2;
drop view if exists 99103_mv3;
drop view if exists 99103_mv4;
drop stream if exists 99103_stream;
