-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json;

SET allow_experimental_object_type = 1;

CREATE STREAM t_json(id uint64, obj JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json format JSONEachRow {"id": 1, "obj": {"foo": 1, "k1": 2}};
INSERT INTO t_json format JSONEachRow {"id": 2, "obj": {"foo": 1, "k2": 2}};

OPTIMIZE STREAM t_json FINAL;

SELECT any(to_type_name(obj)) from t_json;

DROP STREAM IF EXISTS t_json;
