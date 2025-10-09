SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;

CREATE STREAM table1(a string, b Date) PRIMARY KEY (a, b) order by a;
CREATE STREAM table2(c string, a string, d Date) PRIMARY KEY (c, a, d) order by c;

INSERT INTO table1(a, b) VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2(c, a, d) VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');
SELECT SLEEP(1);

SELECT * FROM table(table1) AS t1 ALL LEFT JOIN (SELECT *, '0.10', c, d AS b FROM table(table2)) AS t2 USING (a, b) ORDER BY d, t1.a ASC FORMAT PrettyCompact settings max_rows_in_join = 1;

SELECT pow('0.0000000257', NULL), pow(pow(NULL, NULL), NULL) - NULL, (val + NULL) = (rval * 0), * FROM (SELECT (val + 256) = (NULL * NULL), to_low_cardinality(to_nullable(dummy)) AS val FROM system.one) AS s1 ANY LEFT JOIN (SELECT to_low_cardinality(dummy) AS rval FROM system.one) AS s2 ON (val + 0) = (rval * 255) settings max_rows_in_join = 1;

DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;

CREATE STREAM table1(a string, b Date) ENGINE MergeTree order by a;
CREATE STREAM table2(c string, a string, d Date) ENGINE MergeTree order by c;

INSERT INTO table1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

set join_algorithm = 'partial_merge';

SELECT * FROM table1 AS t1 ALL LEFT JOIN (SELECT *, '0.10', c, d AS b FROM table2) AS t2 USING (a, b) ORDER BY d, t1.a ASC FORMAT PrettyCompact settings max_rows_in_join = 1;

SELECT pow('0.0000000257', NULL), pow(pow(NULL, NULL), NULL) - NULL, (val + NULL) = (rval * 0), * FROM (SELECT (val + 256) = (NULL * NULL), to_low_cardinality(to_nullable(dummy)) AS val FROM system.one) AS s1 ANY LEFT JOIN (SELECT to_low_cardinality(dummy) AS rval FROM system.one) AS s2 ON (val + 0) = (rval * 255) settings max_rows_in_join = 1;

DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;