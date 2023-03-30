DROP STREAM IF EXISTS t1;

DROP STREAM IF EXISTS any_left_join;
DROP STREAM IF EXISTS any_inner_join;
DROP STREAM IF EXISTS any_right_join;
DROP STREAM IF EXISTS any_full_join;

DROP STREAM IF EXISTS semi_left_join;
DROP STREAM IF EXISTS semi_right_join;
DROP STREAM IF EXISTS anti_left_join;
DROP STREAM IF EXISTS anti_right_join;

CREATE STREAM t1 (x uint32, str string) engine = Memory;

CREATE STREAM any_left_join (x uint32, s string) engine = Join(ANY, LEFT, x);
CREATE STREAM any_inner_join (x uint32, s string) engine = Join(ANY, INNER, x);
CREATE STREAM any_right_join (x uint32, s string) engine = Join(ANY, RIGHT, x);

CREATE STREAM semi_left_join (x uint32, s string) engine = Join(SEMI, LEFT, x);
CREATE STREAM semi_right_join (x uint32, s string) engine = Join(SEMI, RIGHT, x);

CREATE STREAM anti_left_join (x uint32, s string) engine = Join(ANTI, LEFT, x);
CREATE STREAM anti_right_join (x uint32, s string) engine = Join(ANTI, RIGHT, x);

INSERT INTO t1 (x, str) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5');

INSERT INTO any_left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO any_inner_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO any_right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

INSERT INTO semi_left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO semi_right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO anti_left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO anti_right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SET join_use_nulls = 0;
SET any_join_distinct_right_table_keys = 0;

SELECT 'any left';
SELECT * FROM t1 ANY LEFT JOIN any_left_join j USING(x) ORDER BY x, str, s;

SELECT 'any inner';
SELECT * FROM t1 ANY INNER JOIN any_inner_join j USING(x) ORDER BY x, str, s;

SELECT 'any right';
SELECT * FROM t1 ANY RIGHT JOIN any_right_join j USING(x) ORDER BY x, str, s;

SELECT 'semi left';
SELECT * FROM t1 SEMI LEFT JOIN semi_left_join j USING(x) ORDER BY x, str, s;

SELECT 'semi right';
SELECT * FROM t1 SEMI RIGHT JOIN semi_right_join j USING(x) ORDER BY x, str, s;

SELECT 'anti left';
SELECT * FROM t1 ANTI LEFT JOIN anti_left_join j USING(x) ORDER BY x, str, s;

SELECT 'anti right';
SELECT * FROM t1 ANTI RIGHT JOIN anti_right_join j USING(x) ORDER BY x, str, s;

-- run queries once more time (issue #16991)

SELECT 'any left';
SELECT * FROM t1 ANY LEFT JOIN any_left_join j USING(x) ORDER BY x, str, s;

SELECT 'any inner';
SELECT * FROM t1 ANY INNER JOIN any_inner_join j USING(x) ORDER BY x, str, s;

SELECT 'any right';
SELECT * FROM t1 ANY RIGHT JOIN any_right_join j USING(x) ORDER BY x, str, s;

SELECT 'semi left';
SELECT * FROM t1 SEMI LEFT JOIN semi_left_join j USING(x) ORDER BY x, str, s;

SELECT 'semi right';
SELECT * FROM t1 SEMI RIGHT JOIN semi_right_join j USING(x) ORDER BY x, str, s;

SELECT 'anti left';
SELECT * FROM t1 ANTI LEFT JOIN anti_left_join j USING(x) ORDER BY x, str, s;

SELECT 'anti right';
SELECT * FROM t1 ANTI RIGHT JOIN anti_right_join j USING(x) ORDER BY x, str, s;

DROP STREAM t1;

DROP STREAM any_left_join;
DROP STREAM any_inner_join;
DROP STREAM any_right_join;

DROP STREAM semi_left_join;
DROP STREAM semi_right_join;
DROP STREAM anti_left_join;
DROP STREAM anti_right_join;
