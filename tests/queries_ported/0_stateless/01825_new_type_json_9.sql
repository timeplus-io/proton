-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json;

SET enable_json_type = 1;

CREATE STREAM t_json(id uint64, obj JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json format JSONEachRow {"id": 1, "obj": {"foo": 1, "k1": 2}};

INSERT INTO t_json format JSONEachRow {"id": 2, "obj": {"foo": 1, "k2": 2}};

OPTIMIZE STREAM t_json FINAL;

SELECT distinct array_join(json_all_paths_with_types(obj)) as path from t_json order by path;

DROP STREAM IF EXISTS t_json;
