-- Tags: no-fasttest

DROP STREAM IF EXISTS t_flatten_tuple;
DROP STREAM IF EXISTS t_flatten_object;

SET flatten_nested = 0;

CREATE STREAM t_flatten_tuple(t Tuple(t1 Nested(a uint32, s string), b uint32, t2 Tuple(k string, v uint32))) ENGINE = Memory;

INSERT INTO t_flatten_tuple VALUES (([(1, 'a'), (2, 'b')], 3, ('c', 4)));

SELECT flattenTuple(t) AS ft, to_type_name(ft) FROM t_flatten_tuple;

SET allow_experimental_object_type = 1;
CREATE STREAM t_flatten_object(data JSON) ENGINE = Memory;

INSERT INTO t_flatten_object VALUES ('{"id": 1, "obj": {"k1": 1, "k2": {"k3": 2, "k4": [{"k5": 3}, {"k5": 4}]}}, "s": "foo"}');
INSERT INTO t_flatten_object VALUES ('{"id": 2, "obj": {"k2": {"k3": "str", "k4": [{"k6": 55}]}, "some": 42}, "s": "bar"}');

SELECT to_type_name(data), to_type_name(flattenTuple(data)) FROM t_flatten_object LIMIT 1;
SELECT untuple(flattenTuple(data)) FROM t_flatten_object ORDER BY data.id;

DROP STREAM IF EXISTS t_flatten_tuple;
DROP STREAM IF EXISTS t_flatten_object;
