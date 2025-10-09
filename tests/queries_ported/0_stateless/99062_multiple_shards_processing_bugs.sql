DROP VIEW IF EXISTS 99062_mv;
DROP STREAM IF EXISTS 99062_stream;

CREATE STREAM 99062_stream(id int, v string) settings shards=3;
CREATE MATERIALIZED VIEW 99062_mv as select count() from 99062_stream emit on update;

SELECT sleep(1) format Null;
INSERT INTO 99062_stream(id, v) VALUES(1, 'a');
SELECT sleep(2) format Null;

select * except _tp_time from table(99062_mv) order by _tp_time;

DROP VIEW 99062_mv;
DROP STREAM 99062_stream;
