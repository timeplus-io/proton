SET allow_experimental_analyzer = 1;
SET optimize_syntax_fuse_functions = 1, optimize_fuse_sum_count_avg = 1;

DROP STREAM IF EXISTS fuse_tbl;

CREATE STREAM fuse_tbl(a nullable(int8), b int8) Engine = Log;

INSERT INTO fuse_tbl VALUES (1, 1), (2, 2), (NULL, 3);

SELECT avg(a), sum(a) FROM (SELECT a FROM fuse_tbl);
SELECT avg(a), sum(a) FROM (SELECT a FROM fuse_tbl WHERE is_null(a));
SELECT avg(a), sum(a) FROM (SELECT a FROM fuse_tbl WHERE isNotNull(a));

SELECT avg(b), sum(b) FROM (SELECT b FROM fuse_tbl);
SELECT avg(b) * 3, sum(b) + 1 + count(b), count(b) * count(b), count() FROM (SELECT b FROM fuse_tbl);

SELECT sum(b), count(b) from (SELECT x as b FROM (SELECT sum(b) as x, count(b)  FROM fuse_tbl) );

SELECT sum(a + 1), sum(b), count(b), avg(b), count(a + 1), sum(a + 2), count(a) from fuse_tbl SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(a + 1), sum(b), count(b), avg(b), count(a + 1), sum(a + 2), count(a) from fuse_tbl;

EXPLAIN QUERY TREE run_passes = 1 SELECT sum(a), avg(a) from fuse_tbl;
EXPLAIN QUERY TREE run_passes = 1 SELECT sum(b), avg(b) from fuse_tbl;
EXPLAIN QUERY TREE run_passes = 1 SELECT sum(a + 1), sum(b), count(b), avg(b), count(a + 1), sum(a + 2), count(a) from fuse_tbl;
EXPLAIN QUERY TREE run_passes = 1 SELECT avg(b) * 3, sum(b) + 1 + count(b), count(b) * count(b) FROM (SELECT b FROM fuse_tbl);

EXPLAIN QUERY TREE run_passes = 1 SELECT sum(b), count(b) from (SELECT x as b FROM (SELECT sum(b) as x, count(b)  FROM fuse_tbl) );

SELECT sum(x), count(x), avg(x) FROM (SELECT number :: decimal32(0) AS x FROM numbers(0)) SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(x), count(x), avg(x) FROM (SELECT number :: decimal32(0) AS x FROM numbers(0));

SELECT sum(x), count(x), avg(x), to_type_name(sum(x)), to_type_name(count(x)), to_type_name(avg(x)) FROM (SELECT number :: decimal32(0) AS x FROM numbers(10)) SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(x), count(x), avg(x), to_type_name(sum(x)), to_type_name(count(x)), to_type_name(avg(x)) FROM (SELECT number :: decimal32(0) AS x FROM numbers(10));

-- TODO: uncomment after https://github.com/ClickHouse/ClickHouse/pull/43372
-- SELECT avg(b), x - 2 AS b FROM (SELECT number as x FROM numbers(1)) GROUP BY x;

DROP STREAM fuse_tbl;
