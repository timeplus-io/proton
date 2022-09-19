DROP STREAM IF EXISTS l;
DROP STREAM IF EXISTS r;

create stream l (a string, b tuple(string, string)) ();
create stream r (a string, c tuple(string, string)) ();

INSERT INTO l (a, b) VALUES ('a', ('b', 'c')), ('d', ('e', 'f'));
INSERT INTO r (a, c) VALUES ('a', ('b', 'c')), ('x', ('y', 'z'));

SET join_use_nulls = 0;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT a from l RIGHT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

SET join_use_nulls = 1;
SELECT a from l LEFT JOIN r USING a ORDER BY a;
SELECT a from l RIGHT JOIN r USING a ORDER BY a;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

DROP STREAM l;
DROP STREAM r;

create stream l (a string, b string) ();
create stream r (a string, c array(string)) ();

INSERT INTO l (a, b) VALUES ('a', 'b'), ('d', 'e');
INSERT INTO r (a, c) VALUES ('a', ['b', 'c']), ('x', ['y', 'z']);

SET join_use_nulls = 0;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

SET join_use_nulls = 1;
SELECT a from l LEFT JOIN r USING a ORDER BY a;
SELECT a from l RIGHT JOIN r USING a ORDER BY a;
SELECT * from l LEFT JOIN r USING a ORDER BY a;
SELECT * from l RIGHT JOIN r USING a ORDER BY a;

DROP STREAM l;
DROP STREAM r;
