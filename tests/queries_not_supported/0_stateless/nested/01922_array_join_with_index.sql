DROP STREAM IF EXISTS t_array_index;

create stream t_array_index (n nested(key string, value string))
ENGINE = MergeTree ORDER BY n.key;

INSERT INTO t_array_index VALUES (['a', 'b'], ['c', 'd']);

SELECT * FROM t_array_index ARRAY JOIN n WHERE n.key = 'a';

DROP STREAM IF EXISTS t_array_index;
