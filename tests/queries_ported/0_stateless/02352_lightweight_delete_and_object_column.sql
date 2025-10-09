DROP STREAM IF EXISTS t_obj SYNC;

CREATE STREAM t_obj(id int32, name JSON) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_obj select number, '{"a" : "' || to_string(number) || '"}' FROM numbers(100);

DELETE FROM t_obj WHERE id = 10;

SELECT COUNT() FROM t_obj;

DROP STREAM t_obj SYNC;
