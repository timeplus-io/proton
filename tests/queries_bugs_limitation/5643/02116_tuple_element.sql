SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS t_tuple_element;

CREATE STREAM t_tuple_element(t1 tuple(a uint32, s string), t2 tuple(uint32, string)) ENGINE = Memory;
INSERT INTO t_tuple_element VALUES ((1, 'a'), (2, 'b'));

SET optimize_functions_to_subcolumns = 1;

SELECT t1.1 FROM t_tuple_element;
EXPLAIN SYNTAX SELECT t1.1 FROM t_tuple_element;

SELECT tuple_element(t1, 2) FROM t_tuple_element;
EXPLAIN SYNTAX SELECT tuple_element(t1, 2) FROM t_tuple_element;

SELECT tuple_element(t1, 'a') FROM t_tuple_element;
EXPLAIN SYNTAX SELECT tuple_element(t1, 'a') FROM t_tuple_element;

SELECT tuple_element(number, 1) FROM numbers(1); -- { serverError 43 }
SELECT tuple_element(t1) FROM t_tuple_element; -- { serverError 42 }
SELECT tuple_element(t1, 'b') FROM t_tuple_element; -- { serverError 10 }
SELECT tuple_element(t1, 0) FROM t_tuple_element; -- { serverError 127 }
SELECT tuple_element(t1, 3) FROM t_tuple_element; -- { serverError 127 }
SELECT tuple_element(t1, materialize('a')) FROM t_tuple_element; -- { serverError 43 }

SELECT t2.1 FROM t_tuple_element;
EXPLAIN SYNTAX SELECT t2.1 FROM t_tuple_element;

SELECT tuple_element(t2, 1) FROM t_tuple_element;
EXPLAIN SYNTAX SELECT tuple_element(t2, 1) FROM t_tuple_element;

SELECT tuple_element(t2) FROM t_tuple_element; -- { serverError 42 }
SELECT tuple_element(t2, 'a') FROM t_tuple_element; -- { serverError 10 }
SELECT tuple_element(t2, 0) FROM t_tuple_element; -- { serverError 127 }
SELECT tuple_element(t2, 3) FROM t_tuple_element; -- { serverError 127 }
SELECT tuple_element(t2, materialize(1)) FROM t_tuple_element; -- { serverError 43 }

DROP STREAM t_tuple_element;

WITH (1, 2) AS t SELECT t.1, t.2;
EXPLAIN SYNTAX WITH (1, 2) AS t SELECT t.1, t.2;

WITH (1, 2)::tuple(a uint32, b uint32) AS t SELECT t.1, tuple_element(t, 'b');
EXPLAIN SYNTAX WITH (1, 2)::tuple(a uint32, b uint32) AS t SELECT t.1, tuple_element(t, 'b');