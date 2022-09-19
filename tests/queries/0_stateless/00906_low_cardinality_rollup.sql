DROP STREAM if exists lc;
create stream lc (a LowCardinality(Nullable(string)), b LowCardinality(Nullable(string))) ENGINE = MergeTree order by tuple();
INSERT INTO lc VALUES ('a', 'b');
INSERT INTO lc VALUES ('c', 'd');

SELECT a, b, count(a) FROM lc GROUP BY a, b WITH ROLLUP ORDER BY a, b;
SELECT a, count(a) FROM lc GROUP BY a WITH ROLLUP ORDER BY a;

SELECT a, b, count(a) FROM lc GROUP BY a, b WITH CUBE ORDER BY a, b;
SELECT a, count(a) FROM lc GROUP BY a WITH CUBE ORDER BY a;

DROP STREAM if exists lc;
