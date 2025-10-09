drop view if exists 99080_mv;
drop stream if exists 99080_stream;

create stream 99080_stream(id int) settings shards=3, sharding_expr='weak_hash32(id)';

CREATE MATERIALIZED VIEW 99080_mv AS
SELECT
  id, min(_tp_time) as min_ts, max(_tp_time) as max_ts, count() as cnt
FROM
  99080_stream
GROUP BY
  id
EMIT AFTER KEY EXPIRE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s
SETTINGS
  default_hash_table = 'hybrid', max_hot_keys = 3, allow_independent_shard_processing = true
STORAGE_SETTINGS flush_threshold_count = 1;

--- Unsupported: pure memory mode
CREATE MATERIALIZED VIEW 99080_mv2 AS SELECT id, min(_tp_time) as min_ts, max(_tp_time) as max_ts, count() as cnt FROM 99080_stream GROUP BY id EMIT AFTER KEY EXPIRE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s; -- { serverError UNSUPPORTED }
--- Unsupported: substream aggregating
CREATE MATERIALIZED VIEW 99080_mv3 AS SELECT id, min(_tp_time) as min_ts, max(_tp_time) as max_ts, count() as cnt FROM 99080_stream partition by id GROUP BY id EMIT AFTER KEY EXPIRE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s; -- { serverError UNSUPPORTED }
--- Unsupported: parallel aggregating without shuffled
CREATE MATERIALIZED VIEW 99080_mv4 AS SELECT id, min(_tp_time) as min_ts, max(_tp_time) as max_ts, count() as cnt FROM 99080_stream GROUP BY id EMIT AFTER KEY EXPIRE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s; -- { serverError UNSUPPORTED }

select sleep(2) format Null;
insert into 99080_stream(id, _tp_time) values (1, '2025-06-18 00:00:00.001')(2, '2025-06-18 00:00:00.001')(3, '2025-06-18 00:00:00.001')(4, '2025-06-18 00:00:00.001')(5, '2025-06-18 00:00:00.001');

--- Validate checkpoint
system pause materialized view 99080_mv;
system resume materialized view 99080_mv;

insert into 99080_stream(id, _tp_time) values (1, '2025-06-18 00:00:01.001')(2, '2025-06-18 00:00:01.001')(3, '2025-06-18 00:00:01.001')(4, '2025-06-18 00:00:01.001')(5, '2025-06-18 00:00:01.001');

select sleep(2) format Null;
select * except _tp_time from table(99080_mv) order by id;

drop view if exists 99080_mv;
drop stream if exists 99080_stream;
