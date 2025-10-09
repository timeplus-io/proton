DROP VIEW if exists 99024_stateful_with_partition;
DROP VIEW if exists 99024_aggr_with_partition;
DROP VIEW if exists 99024_simple_stateful_over;
DROP VIEW if exists 99024_simple_aggr_over;
DROP VIEW if exists 99024_complex_stateful_over;
DROP VIEW if exists 99024_complex_aggr_over;
DROP VIEW if exists 99024_aggr_and_rank_over;
DROP STREAM if exists 99024_stream;

CREATE STREAM 99024_stream(id int, value float) settings shards=3;
CREATE MATERIALIZED VIEW 99024_stateful_with_partition AS SELECT id, value, lag(value), round(value - lag(value), 1) FROM 99024_stream PARTITION BY id settings checkpoint_interval=2;
CREATE MATERIALIZED VIEW 99024_aggr_with_partition AS SELECT id, max(value), min(value), round(max(value) - min(value), 1) FROM 99024_stream PARTITION BY id emit periodic 1ms settings checkpoint_interval=2;
CREATE MATERIALIZED VIEW 99024_simple_stateful_over AS SELECT id, value, lag(value) over (partition by id) as lag_v, round(value - lag_v, 1) FROM 99024_stream settings checkpoint_interval=2;
CREATE MATERIALIZED VIEW 99024_simple_aggr_over AS SELECT id, max(value) over (partition by id) as max,  min(value) over (partition by id) as min FROM 99024_stream settings checkpoint_interval=2; --- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW 99024_complex_stateful_over AS SELECT id, value, lag(value) over (partition by id) as lag_v1, lag(value) over (partition by id % 1) as lag_v2, round(value - lag_v, 1), round(value - lag_v2, 1) FROM 99024_stream settings checkpoint_interval=2; --- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW 99024_complex_aggr_over AS SELECT id, min(value) over (partition by id) as sum_v1, max(value) over (partition by id % 1) as sum_v2 FROM 99024_stream settings checkpoint_interval=2; --- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW 99024_aggr_and_rank_over AS SELECT id, round(sum(value), 1) as sum_v, row_number() over (order by sum_v desc) as rank FROM 99024_stream group by id settings checkpoint_interval=2; --- { serverError NOT_IMPLEMENTED }

SELECT sleep(2) FORMAT Null;

INSERT INTO 99024_stream(id, value) values(1, 1.1);
SELECT sleep(1) FORMAT Null;
INSERT INTO 99024_stream(id, value) values(2, 22.2);
SELECT sleep(1) FORMAT Null;
INSERT INTO 99024_stream(id, value) values(1, 3.3);
SELECT sleep(1) FORMAT Null;
INSERT INTO 99024_stream(id, value) values(1, 4.4);
SELECT sleep(1) FORMAT Null;
INSERT INTO 99024_stream(id, value) values(2, 66.6);

SELECT sleep(3) FORMAT Null;

SELECT '======stateful_with_partition======' as separator;
SELECT 'historical:' as separator;
with table as (select * from table(99024_stream) order by _tp_time) SELECT id, value, lag(value), round(value - lag(value), 1) FROM table PARTITION BY id;
SELECT 'streaming:' as separator;
SELECT * except _tp_time from table(99024_stateful_with_partition) order by _tp_time;
SELECT '======aggr_with_partition======' as separator;
SELECT 'historical:' as separator;
with table as (select * from table(99024_stream) order by _tp_time) SELECT id, max(value), min(value), round(max(value) - min(value), 1) FROM table PARTITION BY id order by id;
SELECT 'streaming:' as separator;
SELECT * except _tp_time from table(99024_aggr_with_partition) order by _tp_time;
SELECT '======simple_stateful_over======' as separator;
SELECT 'historical:' as separator;
with table as (select * from table(99024_stream) order by _tp_time) SELECT id, value, lag(value) over (partition by id) as lag_v, round(value - lag_v, 1) FROM table;
SELECT 'streaming:' as separator;
SELECT * except _tp_time from table(99024_simple_stateful_over) order by _tp_time;
SELECT '======complex_stateful_over======' as separator;
SELECT 'historical:' as separator;
with table as (select * from table(99024_stream) order by _tp_time) SELECT id, lag(value) over (partition by id) as lag_v1, lag(value) over (partition by id % 1) as lag_v2 FROM table; --- { serverError NOT_IMPLEMENTED }
SELECT 'streaming:' as separator;
SELECT * except _tp_time from table(99024_complex_stateful_over) order by _tp_time; --- { serverError UNKNOWN_STREAM }
SELECT '======complex_aggr_over======' as separator;
SELECT 'historical:' as separator;
with table as (select * from table(99024_stream) order by _tp_time) SELECT id, id % 1, min(value) over (partition by id) as sum_v1, max(value) over (partition by id % 1) as sum_v2 FROM table order by id;
SELECT 'streaming:' as separator;
SELECT * except _tp_time from table(99024_complex_aggr_over) order by _tp_time; --- { serverError UNKNOWN_STREAM }
SELECT '======aggr_and_rank_over======' as separator;
SELECT 'historical:' as separator;
with table as (select * from table(99024_stream) order by _tp_time) SELECT id, round(sum(value)) as sum_v, row_number() over (order by sum_v desc) as rank FROM table group by id;
SELECT 'streaming:' as separator;
SELECT * except _tp_time from table(99024_aggr_and_rank_over) order by _tp_time; --- { serverError UNKNOWN_STREAM }


DROP VIEW if exists 99024_stateful_with_partition;
DROP VIEW if exists 99024_aggr_with_partition;
DROP VIEW if exists 99024_simple_stateful_over;
DROP VIEW if exists 99024_simple_aggr_over;
DROP VIEW if exists 99024_complex_stateful_over;
DROP VIEW if exists 99024_complex_aggr_over;
DROP VIEW if exists 99024_aggr_and_rank_over;
DROP STREAM if exists 99024_stream;
