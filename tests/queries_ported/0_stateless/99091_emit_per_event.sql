drop view if exists 99091_mv;
drop view if exists 99091_mv2;
drop view if exists 99091_mv3;
drop view if exists 99091_mv4;
drop view if exists 99091_mv5;
drop view if exists 99091_mv6;
drop stream if exists 99091_stream;
drop stream if exists 99091_multi_shards_stream;

create stream 99091_stream(id int);
create stream 99091_multi_shards_stream(id int) settings shards=3, sharding_expr='to_int(id)';

--- Memory global aggr without key
CREATE MATERIALIZED VIEW 99091_mv AS SELECT emit_version() as version, count() FROM 99091_stream EMIT PER EVENT
STORAGE_SETTINGS flush_threshold_count = 1;
--- Memory global aggr with key
CREATE MATERIALIZED VIEW 99091_mv2 AS SELECT emit_version() as version, id, count() FROM 99091_multi_shards_stream group by id EMIT PER EVENT settings allow_independent_shard_processing=true
STORAGE_SETTINGS flush_threshold_count = 1;
--- Memory window aggr
CREATE MATERIALIZED VIEW 99091_mv3 AS SELECT emit_version() as version, window_start as win_start, window_end as win_end, id, count() FROM tumble(99091_multi_shards_stream, 2s) group by window_start, window_end, id EMIT PER EVENT settings allow_independent_shard_processing=true
STORAGE_SETTINGS flush_threshold_count = 1;

--- Hybrid global aggr without key
CREATE MATERIALIZED VIEW 99091_mv4 AS SELECT emit_version() as version, count() FROM 99091_stream EMIT PER EVENT settings default_hash_table='hybrid', max_hot_keys=1
STORAGE_SETTINGS flush_threshold_count = 1;
--- Hybrid global aggr with key
CREATE MATERIALIZED VIEW 99091_mv5 AS SELECT emit_version() as version, id, count() FROM 99091_multi_shards_stream group by id EMIT PER EVENT settings default_hash_table='hybrid', max_hot_keys=1, allow_independent_shard_processing=true
STORAGE_SETTINGS flush_threshold_count = 1;
--- Hybrid window aggr
CREATE MATERIALIZED VIEW 99091_mv6 AS SELECT emit_version() as version, window_start as win_start, window_end as win_end, id, count() FROM tumble(99091_multi_shards_stream, 2s) group by window_start, window_end, id EMIT PER EVENT settings default_hash_table='hybrid', max_hot_keys=1, allow_independent_shard_processing=true
STORAGE_SETTINGS flush_threshold_count = 1;

--- Unsupported: substream aggregating
CREATE MATERIALIZED VIEW 99091_mv7 AS SELECT emit_version() as version, id, count() as cnt FROM 99091_stream partition by id GROUP BY id EMIT PER EVENT; -- { serverError UNSUPPORTED }
--- Unsupported: parallel aggregating without shuffled
CREATE MATERIALIZED VIEW 99091_mv8 AS SELECT emit_version() as version, id, count() as cnt FROM 99091_multi_shards_stream GROUP BY id EMIT PER EVENT; -- { serverError UNSUPPORTED }
--- Unsupported: session window aggregating
CREATE MATERIALIZED VIEW 99091_mv9 AS SELECT emit_version() as version, window_start as win_start, window_end as win_end, id, count() as cnt FROM session(99091_stream, 10s) GROUP BY window_start, window_end, id EMIT PER EVENT; -- { serverError UNSUPPORTED }

select sleep(2) format Null;
insert into 99091_stream(id, _tp_time) values (1, '2025-07-10 00:00:00')(2, '2025-07-10 00:00:01')(2, '2025-07-10 00:00:02');
insert into 99091_multi_shards_stream(id, _tp_time) values (1, '2025-07-10 00:00:00')(2, '2025-07-10 00:00:01')(2, '2025-07-10 00:00:02');

--- Validate checkpoint
system pause materialized view 99091_mv;
system pause materialized view 99091_mv2;
system pause materialized view 99091_mv3;
system pause materialized view 99091_mv4;
system pause materialized view 99091_mv5;
system pause materialized view 99091_mv6;
system resume materialized view 99091_mv;
system resume materialized view 99091_mv2;
system resume materialized view 99091_mv3;
system resume materialized view 99091_mv4;
system resume materialized view 99091_mv5;
system resume materialized view 99091_mv6;

insert into 99091_stream(id, _tp_time) values (1, '2025-07-10 00:00:01')(2, '2025-07-10 00:00:02')(1, '2025-07-10 00:00:02');
insert into 99091_multi_shards_stream(id, _tp_time) values (1, '2025-07-10 00:00:01')(2, '2025-07-10 00:00:02')(1, '2025-07-10 00:00:02');

select sleep(3) format Null;
select * except _tp_time from table(99091_mv) order by version;
select * except _tp_time from table(99091_mv2) order by version, id;
select * except _tp_time from table(99091_mv3) order by version, id;
select * except _tp_time from table(99091_mv4) order by version;
select * except _tp_time from table(99091_mv5) order by version, id;
select * except _tp_time from table(99091_mv6) order by version, id;

drop view if exists 99091_mv;
drop view if exists 99091_mv2;
drop view if exists 99091_mv3;
drop view if exists 99091_mv4;
drop view if exists 99091_mv5;
drop view if exists 99091_mv6;
drop stream if exists 99091_stream;
drop stream if exists 99091_multi_shards_stream;
