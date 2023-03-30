DROP STREAM IF EXISTS stream1;
DROP STREAM IF EXISTS stream2;

CREATE STREAM stream1(a string, b Date) ENGINE MergeTree order by a;
CREATE STREAM stream2(c string, a string, d Date) ENGINE MergeTree order by c;

INSERT INTO stream1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO stream2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

SELECT * FROM stream1 t1 FORMAT PrettyCompact;
SELECT *, c as a, d as b FROM stream2 FORMAT PrettyCompact;
SELECT * FROM stream1 t1 ALL LEFT JOIN (SELECT *, c, d as b FROM stream2) t2 USING (a, b) ORDER BY d, t1.a FORMAT PrettyCompact;
SELECT * FROM stream1 t1 ALL INNER JOIN (SELECT *, c, d as b FROM stream2) t2 USING (a, b) ORDER BY d, t1.a FORMAT PrettyCompact;

DROP STREAM stream1;
DROP STREAM stream2;
