-- Tags: long
DROP STREAM IF EXISTS t_json_parallel;

SET allow_experimental_object_type = 1, max_insert_threads = 20, max_threads = 20, min_insert_block_size_rows = 65536;
CREATE STREAM t_json_parallel (data JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_parallel SELECT materialize('{"k1":1, "k2": "some"}') FROM numbers_mt(500000);
SELECT any(to_type_name(data)), count() FROM t_json_parallel;

DROP STREAM t_json_parallel;
