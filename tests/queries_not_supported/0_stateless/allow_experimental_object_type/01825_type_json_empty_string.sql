-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json_empty_str;
SET allow_experimental_object_type = 1;

CREATE STREAM t_json_empty_str(id uint32, o JSON) ENGINE = Memory;

INSERT INTO t_json_empty_str VALUES (1, ''), (2, '{"k1": 1, "k2": "v1"}'), (3, '{}'), (4, '{"k1": 2}');

SELECT * FROM t_json_empty_str ORDER BY id;

DROP STREAM t_json_empty_str;
