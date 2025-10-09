drop view if exists test_mv_99066;
drop stream if exists test_stream_99066;

create stream test_stream_99066(i int, v float) settings shards=10;

--- Covering WatermarkWithSubstream, AggregatingWithSubstream, ExpressionWithSubstream
create materialized view test_mv_99066 as select i, window_end as win_end, max_v, lag(window_end) as lag_win_end, lag(max_v) as lag_max_v from (select i, window_end, max(v) as max_v from tumble(test_stream_99066, 2s) partition by i group by i, window_end);
create materialized view test_mv2_99066 as select i, window_end as win_end, max_v, lag(window_end) as lag_win_end, lag(max_v) as lag_max_v from (select i, window_end, max(v) as max_v from tumble(test_stream_99066, 2s) partition by i group by i, window_end) settings default_hash_table='hybrid', max_hot_keys=1; -- { serverError UNSUPPORTED }

insert into test_stream_99066(i, v, _tp_time) values(1, 100, '2025-04-30 00:00:10')(2, 200, '2025-04-30 00:00:10');
insert into test_stream_99066(i, v, _tp_time) values(1, 101, '2025-04-30 00:00:11')(2, 201, '2025-04-30 00:00:11');
select sleep(1) format Null;
insert into test_stream_99066(i, v, _tp_time) values(1, 102, '2025-04-30 00:00:12')(2, 202, '2025-04-30 00:00:12');

--- Pause/Resume to validate the checkpoints
system pause materialized view test_mv_99066;
system resume materialized view test_mv_99066;

--- Ignores late event `(2, 203, '2025-04-30 00:00:11')`
insert into test_stream_99066(i, v, _tp_time) values(1, 103, '2025-04-30 00:00:13')(2, 203, '2025-04-30 00:00:11')(3, 303, '2025-04-30 00:00:13');
select sleep(2) format Null;
insert into test_stream_99066(i, v, _tp_time) values(1, 104, '2025-04-30 00:00:14')(2, 204, '2025-04-30 00:00:14')(3, 304, '2025-04-30 00:00:14');

select sleep(3) format Null;
select * except _tp_time from table(test_mv_99066) order by win_end, i asc;

drop view test_mv_99066;
drop stream test_stream_99066;
