DROP VIEW IF EXISTS 99020_mv;
DROP VIEW IF EXISTS 99020_mv_with_target;
DROP VIEW IF EXISTS 99020_view;
DROP STREAM IF EXISTS 99020_stream;
DROP STREAM IF EXISTS 99020_target_stream;

CREATE STREAM 99020_stream (a int, b int);
CREATE STREAM 99020_target_stream (res int, res2 int);

CREATE MATERIALIZED VIEW 99020_mv AS SELECT a+b as res FROM 99020_stream;
CREATE MATERIALIZED VIEW 99020_mv_with_target INTO 99020_target_stream AS SELECT a+b as res FROM 99020_stream;
CREATE VIEW 99020_view AS SELECT a+b as res FROM 99020_stream;

ALTER VIEW 99020_mv MODIFY QUERY SETTING checkpoint_interval='15';

ALTER VIEW 99020_mv MODIFY QUERY SETTING checkpoint_interval=-1;

ALTER VIEW 99020_stream MODIFY SETTING logstore_retention_ms = 3600000; --- { serverError INCORRECT_QUERY }

SELECT sleep(2) FORMAT Null;
insert into 99020_stream(a, b) values (1, 2);

SELECT sleep(2) FORMAT Null;
ALTER VIEW 99020_view MODIFY QUERY SELECT a-b FROM 99020_stream; --- { serverError NOT_IMPLEMENTED }

--- modify expr query without target stream
ALTER VIEW 99020_mv MODIFY QUERY SELECT a-b as res FROM 99020_stream;

--- modify query with cte subquery
ALTER VIEW 99020_mv MODIFY QUERY WITH t AS (SELECT a-b as res FROM 99020_stream) SELECT * FROM t;

--- add expr query without target stream
ALTER VIEW 99020_mv MODIFY QUERY SELECT a-b as res, a+b as res2 FROM 99020_stream;

--- modify expr query with target stream
ALTER VIEW 99020_mv_with_target MODIFY QUERY SELECT a-b as res FROM 99020_stream;

--- add expr query with with target stream
ALTER VIEW 99020_mv_with_target MODIFY QUERY SELECT a+b as res, a-b as res2 FROM 99020_stream;

SELECT 'without target columns after alter:';
SELECT name, type
FROM system.columns
WHERE database = current_database() AND table = '99020_mv' AND name NOT IN ('_tp_time', '_tp_sn')
ORDER BY name;

SELECT 'with target columns after alter:';
SELECT name, type
FROM system.columns
WHERE database = current_database() AND table = '99020_mv_with_target' AND name NOT IN ('_tp_time', '_tp_sn')
ORDER BY name;

--- modify stateful expr query with target stream
ALTER VIEW 99020_mv_with_target MODIFY QUERY SELECT count() as res FROM 99020_stream; --- { serverError UNSUPPORTED }

SELECT sleep(2) FORMAT Null;
SELECT sleep(2) FORMAT Null;
insert into 99020_stream(a, b) values (1, 2);

SELECT sleep(2) FORMAT Null;
SELECT sleep(2) FORMAT Null;
SELECT * except _tp_time from table(99020_view);
SELECT * except _tp_time from table(99020_mv) order by _tp_time;
SELECT * except _tp_time from table(99020_mv_with_target) order by _tp_time;

DROP VIEW IF EXISTS 99020_view;
DROP VIEW IF EXISTS 99020_mv;
DROP VIEW IF EXISTS 99020_mv_with_target;
DROP VIEW IF EXISTS 99020_mv_with_target_hybrid;
DROP STREAM IF EXISTS 99020_stream;
DROP STREAM IF EXISTS 99020_target_stream;
DROP STREAM IF EXISTS 99020_target_stream_hybrid;

SELECT '=== ALTER VIEW MODIFY QUERY (update aggregates) ===';
CREATE STREAM 99020_stream (a int, b int);
CREATE STREAM 99020_target_stream (res int, res2 int);
CREATE STREAM 99020_target_stream_hybrid (res int, res2 int);
CREATE MATERIALIZED VIEW 99020_mv_with_target INTO 99020_target_stream AS SELECT count() as res FROM 99020_stream emit on update;
CREATE MATERIALIZED VIEW 99020_mv_with_target_hybrid INTO 99020_target_stream_hybrid AS SELECT count() as res FROM 99020_stream emit on update settings default_hash_table='hybrid';
SELECT sleep(2) FORMAT Null;
insert into 99020_stream(a, b) values (1, 2);
SELECT sleep(1) FORMAT Null;

--- add aggregate `sum(a)`
ALTER VIEW 99020_mv_with_target MODIFY QUERY SELECT count() as res, sum(a) as res2 FROM 99020_stream emit on update;
ALTER VIEW 99020_mv_with_target_hybrid MODIFY QUERY SELECT count() as res, sum(a) as res2 FROM 99020_stream emit on update settings default_hash_table='hybrid';
SELECT sleep(2) FORMAT Null;
insert into 99020_stream(a, b) values (1, 2);
SELECT sleep(1) FORMAT Null;

--- remove aggregate `sum(a)` (FAILED)
ALTER VIEW 99020_mv_with_target MODIFY QUERY SELECT count() as res FROM 99020_stream emit on update;
ALTER VIEW 99020_mv_with_target_hybrid MODIFY QUERY SELECT count() as res FROM 99020_stream emit on update settings default_hash_table='hybrid';
SELECT sleep(2) FORMAT Null;
insert into 99020_stream(a, b) values (1, 2);
SELECT sleep(1) FORMAT Null;

--- replace aggregate `sum(a)` with `sum(b)` (TRICK)
--- remove aggregate `sum(a)` (TRICK) and add aggregate `sum(b)`
ALTER VIEW 99020_mv_with_target MODIFY QUERY SELECT count() as res, sum(b) as deleted, sum(b) as res2 FROM 99020_stream emit on update;
ALTER VIEW 99020_mv_with_target_hybrid MODIFY QUERY SELECT count() as res, sum(b) as deleted, sum(b) as res2 FROM 99020_stream emit on update settings default_hash_table='hybrid';
SYSTEM recover materialized view 99020_mv_with_target; --- need to manually recover it since the previous alter request has aborted the mv.
SYSTEM recover materialized view 99020_mv_with_target_hybrid;
SELECT sleep(2) FORMAT Null;
insert into 99020_stream(a, b) values (1, 2);
SELECT sleep(2) FORMAT Null;
SELECT 'memory:';
SELECT * except _tp_time from table(99020_mv_with_target) order by _tp_time;
SELECT 'hybrid:';
SELECT * except _tp_time from table(99020_mv_with_target_hybrid) order by _tp_time;

DROP VIEW IF EXISTS 99020_mv_with_target;
DROP VIEW IF EXISTS 99020_mv_with_target_hybrid;
DROP STREAM IF EXISTS 99020_stream;
DROP STREAM IF EXISTS 99020_target_stream;
DROP STREAM IF EXISTS 99020_target_stream_hybrid;
