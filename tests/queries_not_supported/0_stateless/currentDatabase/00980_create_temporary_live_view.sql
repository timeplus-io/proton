-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS mt;

SELECT name, value from system.settings WHERE name = 'temporary_live_view_timeout';
SELECT name, value from system.settings WHERE name = 'live_view_heartbeat_interval';

create stream mt (a int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv WITH TIMEOUT 1 AS SELECT sum(a) FROM mt;

SHOW TABLES WHERE database=currentDatabase() and name LIKE 'lv';
SELECT sleep(2);
SHOW TABLES WHERE database=currentDatabase() and name LIKE 'lv';

DROP STREAM mt;
