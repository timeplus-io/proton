DROP STREAM IF EXISTS t;

create stream t (a int, b int, c int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number, number * 2, number * 3 FROM numbers(100);

SELECT count() FROM t PREWHERE NOT ignore(a) WHERE b > 0;
SELECT sum(a) FROM t PREWHERE isNotNull(a) WHERE isNotNull(b) AND c > 0;

DROP STREAM t;
