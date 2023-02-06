-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json_partitions;

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

CREATE STREAM t_json_partitions (id uint32, obj JSON)
ENGINE MergeTree ORDER BY id PARTITION BY id;

INSERT INTO t_json_partitions FORMAT JSONEachRow {"id": 1, "obj": {"k1": "v1"}} {"id": 2, "obj": {"k2": "v2"}};

SELECT * FROM t_json_partitions ORDER BY id FORMAT JSONEachRow;

DROP STREAM t_json_partitions;
