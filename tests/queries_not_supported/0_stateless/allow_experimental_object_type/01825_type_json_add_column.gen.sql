-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json_add_column;
SET allow_experimental_object_type = 1;

CREATE STREAM t_json_add_column (id uint64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_add_column VALUES (1);
ALTER STREAM t_json_add_column ADD COLUMN s JSON;

INSERT INTO t_json_add_column VALUES(2, '{"k1": 100}');

SELECT * FROM t_json_add_column ORDER BY id FORMAT JSONEachRow;

ALTER STREAM t_json_add_column DROP COLUMN s;

SELECT * FROM t_json_add_column ORDER BY id FORMAT JSONEachRow;

DROP STREAM t_json_add_column;

DROP STREAM IF EXISTS t_json_add_column;
SET allow_experimental_object_type = 1;

CREATE STREAM t_json_add_column (id uint64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01825_add_column/', 'r1') ORDER BY tuple();

INSERT INTO t_json_add_column VALUES (1);
ALTER STREAM t_json_add_column ADD COLUMN s JSON;

INSERT INTO t_json_add_column VALUES(2, '{"k1": 100}');

SELECT * FROM t_json_add_column ORDER BY id FORMAT JSONEachRow;

ALTER STREAM t_json_add_column DROP COLUMN s;

SELECT * FROM t_json_add_column ORDER BY id FORMAT JSONEachRow;

DROP STREAM t_json_add_column;

