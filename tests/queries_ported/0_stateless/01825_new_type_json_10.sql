-- Tags: no-fasttest

SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP STREAM IF EXISTS t_json_10;
CREATE STREAM t_json_10 (o json) ENGINE = Memory;

INSERT INTO t_json_10 FORMAT JSONAsObject {"a": {"b": 1, "c": [{"d": 10, "e": [31]}, {"d": 20, "e": [63, 127]}]}} {"a": {"b": 2, "c": []}}

INSERT INTO t_json_10 FORMAT JSONAsObject {"a": {"b": 3, "c": [{"f": 20, "e": [32]}, {"f": 30, "e": [64, 128]}]}} {"a": {"b": 4, "c": []}}

SELECT DISTINCT array_join(json_all_paths_with_types(o)) as path FROM t_json_10 order by path;
SELECT DISTINCT array_join(json_all_paths_with_types(array_join(o.a.c.:`array(json)`))) as path FROM t_json_10 order by path;
SELECT o FROM t_json_10 ORDER BY o.a.b FORMAT JSONEachRow;
SELECT o.a.b, o.a.c.:`array(json)`.d, o.a.c.:`array(json)`.e, o.a.c.:`array(json)`.f FROM t_json_10 ORDER BY o.a.b;

DROP STREAM t_json_10;
