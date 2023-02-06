-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json_bools;
SET allow_experimental_object_type = 1;

CREATE STREAM t_json_bools (data JSON) ENGINE = Memory;
INSERT INTO t_json_bools VALUES ('{"k1": true, "k2": false}');
SELECT data, to_type_name(data) FROM t_json_bools;

DROP STREAM t_json_bools;
