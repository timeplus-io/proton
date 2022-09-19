DROP STREAM IF EXISTS nested;

create stream nested (x uint8, n nested(a uint64, b string)) ;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP STREAM IF EXISTS nested;

create stream nested (x uint8, n nested(a uint64, b string))  ;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP STREAM IF EXISTS nested;

create stream nested (x uint8, n nested(a uint64, b string)) ENGINE = StripeLog;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP STREAM IF EXISTS nested;

create stream nested (x uint8, n nested(a uint64, b string)) ;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP STREAM IF EXISTS nested;

create stream nested (x uint8, n nested(a uint64, b string)) ENGINE = MergeTree ORDER BY x;

INSERT INTO nested VALUES (1, [2, 3], ['Hello', 'World']);
INSERT INTO nested VALUES (4, [5], ['Goodbye']);

SELECT * FROM nested ORDER BY x;
SELECT x, n.a FROM nested ORDER BY x;
SELECT n.a, n.b FROM nested ORDER BY n.a;


DROP STREAM nested;
