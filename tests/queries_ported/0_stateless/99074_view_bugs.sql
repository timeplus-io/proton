DROP VIEW IF EXISTS 99074_mv;
DROP VIEW IF EXISTS 99074_v;
DROP STREAM IF EXISTS 99074_stream;

--- Bug: https://github.com/timeplus-io/proton-enterprise/issues/9186
CREATE STREAM 99074_stream(id int, value string);
CREATE VIEW 99074_v AS WITH grouped AS (SELECT group_array(value) as arr from 99074_stream) select arr from grouped emit on update SETTINGS default_hash_table = 'hybrid';
SELECT * FROM 99074_v limit 0;

DROP VIEW IF EXISTS 99074_v;
DROP STREAM IF EXISTS 99074_stream;
