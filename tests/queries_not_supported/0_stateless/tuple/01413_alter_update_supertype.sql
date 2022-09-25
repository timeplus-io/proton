DROP STREAM IF EXISTS t;
create stream t (x uint64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number FROM numbers(10);

SELECT * FROM t;

SET mutations_sync = 1;
ALTER STREAM t UPDATE x = x - 1 WHERE x % 2 = 1;

SELECT '---';
SELECT * FROM t;

DROP STREAM t;
