DROP STREAM IF EXISTS t_nested_with_dots;

CREATE STREAM t_nested_with_dots (n nested(id uint64, `values.id` array(uint64)))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nested_with_dots VALUES ([1], [[1]]);

SELECT * FROM t_nested_with_dots;
SELECT n.values.id FROM t_nested_with_dots;

DROP STREAM IF EXISTS t_nested_with_dots;
SET flatten_nested = 0;

CREATE STREAM t_nested_with_dots (n nested(id uint64, `values.id` array(uint64)))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nested_with_dots VALUES ([(1, [1])]);

SELECT * FROM t_nested_with_dots;
SELECT n.values.id FROM t_nested_with_dots;

DROP STREAM IF EXISTS t_nested_with_dots;

CREATE STREAM t_nested_with_dots (`t.t2` Tuple(`t3.t4.t5` Tuple(`s1.s2` string, `u1.u2` uint64), `s3.s4.s5` string))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nested_with_dots VALUES ((('a', 1), 'b'));

SELECT * FROM t_nested_with_dots;
SELECT t.t2.t3.t4.t5.s1.s2, t.t2.t3.t4.t5.u1.u2 FROM t_nested_with_dots;
SELECT t.t2.t3.t4.t5.s1.s2, t.t2.s3.s4.s5 FROM t_nested_with_dots;

DROP STREAM IF EXISTS t_nested_with_dots;
