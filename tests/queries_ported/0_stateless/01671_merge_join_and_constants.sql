DROP STREAM IF EXISTS stream1;
DROP STREAM IF EXISTS stream2;

CREATE STREAM stream1(a string, b Date) ENGINE MergeTree order by a;
CREATE STREAM stream2(c string, a string, d Date) ENGINE MergeTree order by c;

INSERT INTO stream1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO stream2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

set join_algorithm = 'partial_merge';

SELECT * FROM stream1 AS t1 ALL LEFT JOIN (SELECT *, '0.10', c, d AS b FROM stream2) AS t2 USING (a, b) ORDER BY d, t1.a ASC FORMAT PrettyCompact settings max_rows_in_join = 1;

SELECT pow('0.0000000257', NULL), pow(pow(NULL, NULL), NULL) - NULL, (val + NULL) = (rval * 0), * FROM (SELECT (val + 256) = (NULL * NULL), to_low_cardinality(to_nullable(dummy)) AS val FROM system.one) AS s1 ANY LEFT JOIN (SELECT to_low_cardinality(dummy) AS rval FROM system.one) AS s2 ON (val + 0) = (rval * 255) settings max_rows_in_join = 1;

DROP STREAM IF EXISTS stream1;
DROP STREAM IF EXISTS stream2;
