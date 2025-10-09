SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;

CREATE STREAM table1(a string, b Date) ENGINE MergeTree order by a;
CREATE STREAM table2(c string, a string, d Date) ENGINE MergeTree order by c;

INSERT INTO table1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

SELECT * FROM table1 as t1 FORMAT PrettyCompact;
SELECT *, c as a, d as b FROM table2 FORMAT PrettyCompact;
SELECT * FROM table1 as t1 ALL LEFT JOIN (SELECT *, c, d as b FROM table2) as t2 USING (a, b) ORDER BY d, t1.a FORMAT PrettyCompact;
SELECT * FROM table1 as t1 ALL INNER JOIN (SELECT *, c, d as b FROM table2) as t2 USING (a, b) ORDER BY d, t1.a FORMAT PrettyCompact;

DROP STREAM table1;
DROP STREAM table2;