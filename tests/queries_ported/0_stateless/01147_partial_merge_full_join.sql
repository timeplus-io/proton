DROP STREAM IF EXISTS t0;
DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t0 (x uint32, y uint64) engine = MergeTree ORDER BY (x,y);
CREATE STREAM t1 (x uint32, y uint64) engine = MergeTree ORDER BY (x,y);
CREATE STREAM t2 (x uint32, y uint64) engine = MergeTree ORDER BY (x,y);

INSERT INTO t1 (x, y) VALUES (0, 0);

SET join_algorithm = 'partial_merge';

SELECT 't join none using';
SELECT * FROM t1 ANY RIGHT JOIN t0 USING (x) ORDER BY x; -- { serverError 48 }
SELECT * FROM t1 ANY FULL JOIN t0 USING (x) ORDER BY x; -- { serverError 48 }
SELECT '-';
SELECT * FROM t1 RIGHT JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 FULL JOIN t0 USING (x) ORDER BY x;
SELECT 't join none on';
SELECT * FROM t1 ANY RIGHT JOIN t0 ON t1.x = t0.x ORDER BY x; -- { serverError 48 }
SELECT * FROM t1 ANY FULL JOIN t0 ON t1.x = t0.x ORDER BY x; -- { serverError 48 }
SELECT '-';
SELECT * FROM t1 RIGHT JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 FULL JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT 'none join t using';
SELECT * FROM t0 ANY RIGHT JOIN t1 USING (x); -- { serverError 48 }
SELECT * FROM t0 ANY FULL JOIN t1 USING (x); -- { serverError 48 }
SELECT '-';
SELECT * FROM t0 RIGHT JOIN t1 USING (x);
SELECT '-';
SELECT * FROM t0 FULL JOIN t1 USING (x);
SELECT 'none join t on';
SELECT * FROM t0 ANY RIGHT JOIN t1 ON t1.x = t0.x; -- { serverError 48 }
SELECT * FROM t0 ANY FULL JOIN t1 ON t1.x = t0.x; -- { serverError 48 }
SELECT '-';
SELECT * FROM t0 RIGHT JOIN t1 ON t1.x = t0.x;
SELECT '-';
SELECT * FROM t0 FULL JOIN t1 ON t1.x = t0.x;
SELECT '/none';

SET join_use_nulls = 1;

SELECT 't join none using';
SELECT * FROM t1 ANY RIGHT JOIN t0 USING (x) ORDER BY x; -- { serverError 48 }
SELECT * FROM t1 ANY FULL JOIN t0 USING (x) ORDER BY x; -- { serverError 48 }
SELECT '-';
SELECT * FROM t1 RIGHT JOIN t0 USING (x) ORDER BY x;
SELECT '-';
SELECT * FROM t1 FULL JOIN t0 USING (x) ORDER BY x;
SELECT 't join none on';
SELECT * FROM t1 ANY RIGHT JOIN t0 ON t1.x = t0.x ORDER BY x; -- { serverError 48 }
SELECT * FROM t1 ANY FULL JOIN t0 ON t1.x = t0.x ORDER BY x; -- { serverError 48 }
SELECT '-';
SELECT * FROM t1 RIGHT JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT '-';
SELECT * FROM t1 FULL JOIN t0 ON t1.x = t0.x ORDER BY x;
SELECT 'none join t using';
SELECT * FROM t0 ANY RIGHT JOIN t1 USING (x); -- { serverError 48 }
SELECT * FROM t0 ANY FULL JOIN t1 USING (x); -- { serverError 48 }
SELECT '-';
SELECT * FROM t0 RIGHT JOIN t1 USING (x);
SELECT '-';
SELECT * FROM t0 FULL JOIN t1 USING (x);
SELECT 'none join t on';
SELECT * FROM t0 ANY RIGHT JOIN t1 ON t1.x = t0.x; -- { serverError 48 }
SELECT * FROM t0 ANY FULL JOIN t1 ON t1.x = t0.x; -- { serverError 48 }
SELECT '-';
SELECT * FROM t0 RIGHT JOIN t1 ON t1.x = t0.x;
SELECT '-';
SELECT * FROM t0 FULL JOIN t1 ON t1.x = t0.x;
SELECT '/none';

INSERT INTO t1 (x, y) VALUES (1, 10) (2, 20);
INSERT INTO t1 (x, y) VALUES (4, 40) (3, 30);

INSERT INTO t2 (x, y) VALUES (4, 41) (2, 21) (2, 22);
INSERT INTO t2 (x, y) VALUES (0, 0) (5, 50) (4, 42);

SET join_use_nulls = 0;

SELECT 'all right';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x AND to_uint32(int_div(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x AND to_uint64(t1.x) = int_div(t2.y,10) ORDER BY x, t2.y;

SELECT 'all full';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x AND to_uint32(int_div(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x AND to_uint64(t1.x) = int_div(t2.y,10) ORDER BY x, t2.y;

SET join_use_nulls = 1;

SELECT 'all right';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x AND to_uint32(int_div(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON t1.x = t2.x AND to_uint64(t1.x) = int_div(t2.y,10) ORDER BY x, t2.y;

SELECT 'all full';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x AND to_uint32(int_div(t1.y,10)) = t2.x ORDER BY x, t2.y;
SELECT '-';
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON t1.x = t2.x AND to_uint64(t1.x) = int_div(t2.y,10) ORDER BY x, t2.y;

DROP STREAM t0;
DROP STREAM t1;
DROP STREAM t2;
