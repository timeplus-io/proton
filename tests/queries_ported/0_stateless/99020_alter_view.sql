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

DROP VIEW 99020_view;
DROP VIEW 99020_mv;
DROP VIEW 99020_mv_with_target;
DROP STREAM 99020_stream;
DROP STREAM 99020_target_stream;
