DROP STREAM IF EXISTS t1;

DROP STREAM IF EXISTS left_join;
DROP STREAM IF EXISTS inner_join;
DROP STREAM IF EXISTS right_join;
DROP STREAM IF EXISTS full_join;

create stream t1 (x uint32, str string) engine = Memory;

create stream left_join (x uint32, s string) engine = Join(ALL, LEFT, x);
create stream inner_join (x uint32, s string) engine = Join(ALL, INNER, x);
create stream right_join (x uint32, s string) engine = Join(ALL, RIGHT, x);
create stream full_join (x uint32, s string) engine = Join(ALL, FULL, x);

INSERT INTO t1 (x, str) VALUES (0, 'a1'), (1, 'a2'), (2, 'a3'), (3, 'a4'), (4, 'a5');

INSERT INTO left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO inner_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO full_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SET join_use_nulls = 0;

SELECT 'left';
SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s;

SELECT 'inner';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

SELECT 'full';
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s;

SET join_use_nulls = 1;

SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s; -- { serverError 264 }
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s; -- { serverError 264 }

SELECT 'inner (join_use_nulls mix)';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right (join_use_nulls mix)';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

DROP STREAM left_join;
DROP STREAM inner_join;
DROP STREAM right_join;
DROP STREAM full_join;

create stream left_join (x uint32, s string) engine = Join(ALL, LEFT, x) SETTINGS join_use_nulls = 1;
create stream inner_join (x uint32, s string) engine = Join(ALL, INNER, x) SETTINGS join_use_nulls = 1;
create stream right_join (x uint32, s string) engine = Join(ALL, RIGHT, x) SETTINGS join_use_nulls = 1;
create stream full_join (x uint32, s string) engine = Join(ALL, FULL, x) SETTINGS join_use_nulls = 1;

INSERT INTO left_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO inner_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO right_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');
INSERT INTO full_join (x, s) VALUES (2, 'b1'), (2, 'b2'), (4, 'b3'), (4, 'b4'), (4, 'b5'), (5, 'b6');

SELECT 'left (join_use_nulls)';
SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s;

SELECT 'inner (join_use_nulls)';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right (join_use_nulls)';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

SELECT 'full (join_use_nulls)';
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s;

SET join_use_nulls = 0;

SELECT * FROM t1 LEFT JOIN left_join j USING(x) ORDER BY x, str, s; -- { serverError 264 }
SELECT * FROM t1 FULL JOIN full_join j USING(x) ORDER BY x, str, s; -- { serverError 264 }

SELECT 'inner (join_use_nulls mix2)';
SELECT * FROM t1 INNER JOIN inner_join j USING(x) ORDER BY x, str, s;

SELECT 'right (join_use_nulls mix2)';
SELECT * FROM t1 RIGHT JOIN right_join j USING(x) ORDER BY x, str, s;

DROP STREAM t1;

DROP STREAM left_join;
DROP STREAM inner_join;
DROP STREAM right_join;
DROP STREAM full_join;
