DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

CREATE STREAM t1 (x uint64) ENGINE = TinyLog;
INSERT INTO t1 VALUES (1), (2), (3);

CREATE STREAM t2 (x uint64, value string) ENGINE = TinyLog;
INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (2, 'c');
INSERT INTO t2 VALUES (3, 'd'), (3, 'e'), (4, 'f');

SET max_block_size=3;
SET max_joined_block_size_rows = 2;
SET join_algorithm='partial_merge';

SELECT value FROM t1 LEFT JOIN t2 ON t1.x = t2.x ORDER BY value;
