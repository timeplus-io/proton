DROP STREAM IF EXISTS t_tuple_element_default;

CREATE STREAM t_tuple_element_default(t1 tuple(a uint32, s string), t2 tuple(uint32, string)) ENGINE = Memory;
INSERT INTO t_tuple_element_default VALUES ((1, 'a'), (2, 'b'));

SELECT tupleElement(t1, 'z', 'z') FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'z', 'z') FROM t_tuple_element_default;
SELECT tupleElement(t1, 'z', 0) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'z', 0) FROM t_tuple_element_default;
SELECT tupleElement(t2, 'z', 'z') FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t2, 'z', 'z') FROM t_tuple_element_default;

SELECT tupleElement(t1, 3, 'z') FROM t_tuple_element_default; -- { serverError 127 }
SELECT tupleElement(t1, 0, 'z') FROM t_tuple_element_default; -- { serverError 127 }

DROP STREAM t_tuple_element_default;

SELECT '--------------------';

SELECT tupleElement(array(tuple(1, 2)), 'a', 0); -- { serverError 645 }
SELECT tupleElement(array(tuple(1, 2)), 'a', array(tuple(1, 2), tuple(3, 4))); -- { serverError 190 }
SELECT tupleElement(array(array(tuple(1))), 'a', array(array(1, 2, 3))); -- { serverError 190 }

SELECT tupleElement(array(tuple(1, 2)), 'a', array(tuple(3, 4)));
EXPLAIN SYNTAX SELECT tupleElement(array(tuple(1, 2)), 'a', array(tuple(3, 4)));

SELECT '--------------------';

CREATE STREAM t_tuple_element_default(t1 array(tuple(uint32)), t2 uint32) ENGINE = Memory;

SELECT tupleElement(t1, 'a', array(tuple(1))) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(tuple(1))) FROM t_tuple_element_default;

SELECT '--------------------';

INSERT INTO t_tuple_element_default VALUES ([(1)], 100);

SELECT tupleElement(t1, 'a', array(tuple(0))) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(tuple(0))) FROM t_tuple_element_default;

SELECT tupleElement(t1, 'a', array(0)) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(0)) FROM t_tuple_element_default;

INSERT INTO t_tuple_element_default VALUES ([(2)], 200);

SELECT tupleElement(t1, 'a', array(0)) FROM t_tuple_element_default;
EXPLAIN SYNTAX SELECT tupleElement(t1, 'a', array(0)) FROM t_tuple_element_default;

DROP STREAM t_tuple_element_default;

