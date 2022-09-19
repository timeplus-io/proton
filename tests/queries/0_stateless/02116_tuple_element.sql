DROP STREAM IF EXISTS t_tuple_element;

create stream t_tuple_element(t1 tuple(a uint32, s string), t2 tuple(uint32, string)) ;
INSERT INTO t_tuple_element VALUES ((1, 'a'), (2, 'b'));

SET optimize_functions_to_subcolumns = 1;

SELECT t1.1 FROM t_tuple_element;
EXPLAIN SYNTAX SELECT t1.1 FROM t_tuple_element;

SELECT tupleElement(t1, 2) FROM t_tuple_element;
EXPLAIN SYNTAX SELECT tupleElement(t1, 2) FROM t_tuple_element;

SELECT tupleElement(t1, 'a') FROM t_tuple_element;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a') FROM t_tuple_element;

SELECT tupleElement(number, 1) FROM numbers(1); -- { serverError 43 }
SELECT tupleElement(t1) FROM t_tuple_element; -- { serverError 42 }
SELECT tupleElement(t1, 'b') FROM t_tuple_element; -- { serverError 47 }
SELECT tupleElement(t1, 0) FROM t_tuple_element; -- { serverError 127 }
SELECT tupleElement(t1, 3) FROM t_tuple_element; -- { serverError 127 }
SELECT tupleElement(t1, materialize('a')) FROM t_tuple_element; -- { serverError 43 }

SELECT t2.1 FROM t_tuple_element;
EXPLAIN SYNTAX SELECT t2.1 FROM t_tuple_element;

SELECT tupleElement(t2, 1) FROM t_tuple_element;
EXPLAIN SYNTAX SELECT tupleElement(t2, 1) FROM t_tuple_element;

SELECT tupleElement(t2) FROM t_tuple_element; -- { serverError 42 }
SELECT tupleElement(t2, 'a') FROM t_tuple_element; -- { serverError 47 }
SELECT tupleElement(t2, 0) FROM t_tuple_element; -- { serverError 127 }
SELECT tupleElement(t2, 3) FROM t_tuple_element; -- { serverError 127 }
SELECT tupleElement(t2, materialize(1)) FROM t_tuple_element; -- { serverError 43 }

DROP STREAM t_tuple_element;

WITH (1, 2) AS t SELECT t.1, t.2;
EXPLAIN SYNTAX WITH (1, 2) AS t SELECT t.1, t.2;

WITH (1, 2)::tuple(a uint32, b uint32) AS t SELECT t.1, tupleElement(t, 'b');
EXPLAIN SYNTAX WITH (1, 2)::tuple(a uint32, b uint32) AS t SELECT t.1, tupleElement(t, 'b');
