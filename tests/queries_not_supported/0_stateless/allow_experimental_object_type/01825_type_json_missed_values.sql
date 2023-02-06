-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json;

SET allow_experimental_object_type = 1;

CREATE STREAM t_json(id uint64, obj JSON)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_json;

INSERT INTO t_json SELECT number, '{"k1": 1, "k2": 2}' FROM numbers(1000000);
INSERT INTO t_json VALUES (1000001, '{"foo": 1}');

SELECT to_type_name(obj) FROM t_json LIMIT 1;
SELECT count() FROM t_json WHERE obj.foo != 0;

DROP STREAM IF EXISTS t_json;
