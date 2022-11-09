-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json_mutations;

SET query_mode = 'table';
SET output_format_json_named_tuples_as_objects = 1;
SET mutations_sync = 2;

CREATE STREAM t_json_mutations(id uint32, s string, obj json);

INSERT INTO t_json_mutations(* EXCEPT(_tp_time, _tp_index_time)) VALUES (1, 'q', '{"k1": 1, "k2": 2, "k3": [{"k4": "aaa"}, {"k4": "bbb"}]}');
INSERT INTO t_json_mutations(* EXCEPT(_tp_time, _tp_index_time)) VALUES (2, 'w', '{"k1": 3, "k2": 4, "k3": [{"k4": "ccc"}]}');
INSERT INTO t_json_mutations(* EXCEPT(_tp_time, _tp_index_time)) VALUES (3, 'e', '{"k1": 5, "k2": 6}');

select sleep(2);

SELECT * EXCEPT(_tp_time, _tp_index_time) FROM t_json_mutations ORDER BY id;
ALTER STREAM t_json_mutations DELETE WHERE id = 2;
SELECT * EXCEPT(_tp_time, _tp_index_time) FROM t_json_mutations ORDER BY id;
ALTER STREAM t_json_mutations DROP COLUMN s, DROP COLUMN obj, ADD COLUMN t string DEFAULT 'foo';
SELECT * EXCEPT(_tp_time, _tp_index_time) FROM t_json_mutations ORDER BY id;

DROP STREAM t_json_mutations;
