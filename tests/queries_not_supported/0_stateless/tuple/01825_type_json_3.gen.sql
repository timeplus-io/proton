-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP STREAM IF EXISTS t_json_3;

create stream t_json_3(id uint64, data JSON)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01825_3/t_json_3', 'r1') ORDER BY tuple();


    SYSTEM STOP MERGES t_json_3;


INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 1, "data": {"k1": null}}, {"id": 2, "data": {"k1": "v1", "k2" : 2}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, data.k1, data.k2 FROM t_json_3 ORDER BY id;

SELECT '========';
TRUNCATE TABLE t_json_3;

INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 1, "data": {"k1" : []}} {"id": 2, "data": {"k1" : [{"k2" : "v1", "k3" : "v3"}, {"k2" : "v4"}]}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, `data.k1.k2`, `data.k1.k3` FROM t_json_3 ORDER BY id;

INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 3, "data": {"k1" : []}} {"id": 4, "data": {"k1" : []}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, data.k1.k2, data.k1.k3 FROM t_json_3 ORDER BY id;


    SELECT name, column, type
    FROM system.parts_columns
    WHERE table = 't_json_3' AND database = currentDatabase() AND active AND column = 'data'
    ORDER BY name;

    SYSTEM START MERGES t_json_3;
    OPTIMIZE STREAM t_json_3 FINAL;

    SELECT column, type
    FROM system.parts_columns
    WHERE table = 't_json_3' AND database = currentDatabase() AND active AND column = 'data'
    ORDER BY name;


SELECT id, data.k1.k2, data.k1.k3 FROM t_json_3 ORDER BY id;

SELECT '========';
TRUNCATE TABLE t_json_3;

INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 1, "data": {"k1" : {"k2" : 1, "k3" : "foo"}}} {"id": 2, "data": {"k1" : null, "k4" : [1, 2, 3]}}, {"id" : 3, "data": {"k1" : {"k2" : 10}, "k4" : []}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, data.k1.k2, data.k1.k3, data.k4 FROM t_json_3 ORDER BY id;

DROP STREAM t_json_3;

SET allow_experimental_object_type = 1;

DROP STREAM IF EXISTS t_json_3;

create stream t_json_3(id uint64, data JSON)
;



INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 1, "data": {"k1": null}}, {"id": 2, "data": {"k1": "v1", "k2" : 2}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, data.k1, data.k2 FROM t_json_3 ORDER BY id;

SELECT '========';
TRUNCATE TABLE t_json_3;

INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 1, "data": {"k1" : []}} {"id": 2, "data": {"k1" : [{"k2" : "v1", "k3" : "v3"}, {"k2" : "v4"}]}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, `data.k1.k2`, `data.k1.k3` FROM t_json_3 ORDER BY id;

INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 3, "data": {"k1" : []}} {"id": 4, "data": {"k1" : []}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, data.k1.k2, data.k1.k3 FROM t_json_3 ORDER BY id;



SELECT id, data.k1.k2, data.k1.k3 FROM t_json_3 ORDER BY id;

SELECT '========';
TRUNCATE TABLE t_json_3;

INSERT INTO t_json_3 FORMAT JSONEachRow {"id": 1, "data": {"k1" : {"k2" : 1, "k3" : "foo"}}} {"id": 2, "data": {"k1" : null, "k4" : [1, 2, 3]}}, {"id" : 3, "data": {"k1" : {"k2" : 10}, "k4" : []}};

SELECT id, data, to_type_name(data) FROM t_json_3 ORDER BY id;
SELECT id, data.k1.k2, data.k1.k3, data.k4 FROM t_json_3 ORDER BY id;

DROP STREAM t_json_3;

