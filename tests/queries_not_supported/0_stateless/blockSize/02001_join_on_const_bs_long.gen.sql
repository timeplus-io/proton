DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;

create stream t1 (id int) ENGINE = MergeTree ORDER BY id;
create stream t2 (id int) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 SELECT number + 5 AS x FROM (SELECT * FROM system.numbers LIMIT 1111);

SET max_block_size = 100;

SELECT count() == 2222 FROM t1 JOIN t2 ON 1 = 1;

SET max_block_size = 90;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 90 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 95;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 95 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 99;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 99 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 100;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 100 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 101;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 101 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 110;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 110 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 111;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 111 FROM t1 FULL JOIN t2 ON 1 = 2;

SET max_block_size = 128;

SELECT count() == 0 FROM t1 JOIN t2 ON 1 = 2;
SELECT count() == 2 FROM t1 LEFT JOIN t2 ON 1 = 2;
SELECT count() == 1111  FROM t1 RIGHT JOIN t2 ON 1 = 2;
SELECT count() == 1113  FROM t1 FULL JOIN t2 ON 1 = 2;
SELECT max(blockSize()) <= 128 FROM t1 FULL JOIN t2 ON 1 = 2;



DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
