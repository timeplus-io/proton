-- Tags: no-backward-compatibility-check, no-fasttest

DROP STREAM IF EXISTS partslost_0;
DROP STREAM IF EXISTS partslost_1;
DROP STREAM IF EXISTS partslost_2;

CREATE STREAM partslost_0 (x string) ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}_02067_lost/partslost', '0') ORDER BY tuple() SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;

CREATE STREAM partslost_1 (x string) ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}_02067_lost/partslost', '1') ORDER BY tuple() SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;

CREATE STREAM partslost_2 (x string) ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}_02067_lost/partslost', '2') ORDER BY tuple() SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;


INSERT INTO partslost_0 SELECT to_string(number) AS x from system.numbers LIMIT 10000;

ALTER STREAM partslost_0 ADD INDEX idx x TYPE tokenbf_v1(285000, 3, 12345) GRANULARITY 3;

SET mutations_sync = 2;

ALTER STREAM partslost_0 MATERIALIZE INDEX idx;

-- In worst case doesn't check anything, but it's not flaky
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;
select sleep(3) FORMAT Null;

ALTER STREAM partslost_0 DROP INDEX idx;

select count() from partslost_0;
select count() from partslost_1;
select count() from partslost_2;

DROP STREAM IF EXISTS partslost_0;
DROP STREAM IF EXISTS partslost_1;
DROP STREAM IF EXISTS partslost_2;
