DROP STREAM IF EXISTS a;
DROP STREAM IF EXISTS b;
DROP STREAM IF EXISTS c;
DROP STREAM IF EXISTS d;

CREATE STREAM a (k uint64, a1 uint64, a2 string) ENGINE = Memory;
INSERT INTO a VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

CREATE STREAM b (k uint64, b1 uint64, b2 string) ENGINE = Memory;
INSERT INTO b VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

CREATE STREAM c (k uint64, c1 uint64, c2 string) ENGINE = Memory;
INSERT INTO c VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

CREATE STREAM d (k uint64, d1 uint64, d2 string) ENGINE = Memory;
INSERT INTO d VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

SET allow_experimental_analyzer = 1;

-- { echoOn }

EXPLAIN PLAN header = 1
SELECT count() FROM a JOIN b ON b.b1 = a.a1 JOIN c ON c.c1 = b.b1 JOIN d ON d.d1 = c.c1 GROUP BY a.a2
;

EXPLAIN PLAN header = 1
SELECT a.a2, d.d2 FROM a JOIN b USING (k) JOIN c USING (k) JOIN d USING (k)
; -- {serverError 48 }

EXPLAIN PLAN header = 1
SELECT b.bx FROM a
JOIN (SELECT b1, b2 || 'x'  AS bx FROM b ) AS b ON b.b1 = a.a1
JOIN c ON c.c1 = b.b1
JOIN (SELECT number AS d1 from numbers(10)) AS d ON d.d1 = c.c1
WHERE c.c2 != '' ORDER BY a.a2
;

-- { echoOff }

DROP STREAM IF EXISTS a;
DROP STREAM IF EXISTS b;
DROP STREAM IF EXISTS c;
DROP STREAM IF EXISTS d;