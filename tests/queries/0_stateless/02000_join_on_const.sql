DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

create stream t1 (id int) ;
create stream t2 (id int) ;

INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (2), (3);

SELECT 70 = 10 * sum(t1.id) + sum(t2.id) AND count() == 4 FROM t1 JOIN t2 ON 1 = 1;
SELECT 70 = 10 * sum(t1.id) + sum(t2.id) AND count() == 4 FROM t1 JOIN t2 ON 1;
SELECT 70 = 10 * sum(t1.id) + sum(t2.id) AND count() == 4 FROM t1 JOIN t2 ON 2 = 2 AND 3 = 3;
SELECT 70 = 10 * sum(t1.id) + sum(t2.id) AND count() == 4 FROM t1 INNER ANY JOIN t2 ON to_nullable(1);
SELECT 70 = 10 * sum(t1.id) + sum(t2.id) AND count() == 4 FROM t1 INNER ANY JOIN t2 ON toLowCardinality(1);
SELECT 70 = 10 * sum(t1.id) + sum(t2.id) AND count() == 4 FROM t1 INNER ANY JOIN t2 ON toLowCardinality(to_nullable(1));

SELECT * FROM t1 INNER ANY JOIN t2 ON to_nullable(toLowCardinality(1)); -- { serverError 403 }
SELECT * FROM t1 INNER ANY JOIN t2 ON to_uint16(1); -- { serverError 403 }
SELECT * FROM t1 INNER ANY JOIN t2 ON to_int8(1); -- { serverError 403 }
SELECT * FROM t1 INNER ANY JOIN t2 ON 256; -- { serverError 403 }
SELECT * FROM t1 INNER ANY JOIN t2 ON -1; -- { serverError 403 }
SELECT * FROM t1 INNER ANY JOIN t2 ON to_string(1); -- { serverError 403 }

SELECT '- ON NULL -';

SELECT '- inner -';
SELECT * FROM t1 INNER ANY JOIN t2 ON NULL;
SELECT * FROM t1 INNER ANY JOIN t2 ON 0;
SELECT * FROM t1 INNER ANY JOIN t2 ON 1 = 2;
SELECT '- left -';
SELECT * FROM t1 LEFT JOIN t2 ON NULL ORDER BY t1.id, t2.id;
SELECT '- right -';
SELECT * FROM t1 RIGHT JOIN t2 ON NULL ORDER BY t1.id, t2.id;
SELECT '- full -';
SELECT * FROM t1 FULL JOIN t2 ON NULL ORDER BY t1.id, t2.id;

SELECT '- inner -';
SELECT * FROM t1 INNER ANY JOIN t2 ON NULL ORDER BY t1.id NULLS FIRST, t2.id SETTINGS join_use_nulls = 1;
SELECT '- left -';
SELECT * FROM t1 LEFT JOIN t2 ON NULL ORDER BY t1.id NULLS FIRST, t2.id SETTINGS join_use_nulls = 1;
SELECT '- right -';
SELECT * FROM t1 RIGHT JOIN t2 ON NULL ORDER BY t1.id NULLS FIRST, t2.id SETTINGS join_use_nulls = 1;
SELECT '- full -';
SELECT * FROM t1 FULL JOIN t2 ON NULL ORDER BY t1.id NULLS FIRST, t2.id SETTINGS join_use_nulls = 1;

SELECT * FROM t1 JOIN t2 ON 1 = 1 SETTINGS join_algorithm = 'partial_merge'; -- { serverError 48 }
SELECT * FROM t1 JOIN t2 ON 1 = 1 SETTINGS join_algorithm = 'auto'; -- { serverError 48 }
SELECT * FROM t1 JOIN t2 ON NULL SETTINGS join_algorithm = 'partial_merge'; -- { serverError 48 }
SELECT * FROM t1 LEFT JOIN t2 ON NULL SETTINGS join_algorithm = 'partial_merge'; -- { serverError 48 }
SELECT * FROM t1 RIGHT JOIN t2 ON NULL SETTINGS join_algorithm = 'auto'; -- { serverError 48 }
SELECT * FROM t1 FULL JOIN t2 ON NULL SETTINGS join_algorithm = 'partial_merge'; -- { serverError 48 }

DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

