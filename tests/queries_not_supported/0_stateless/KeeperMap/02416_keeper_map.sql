-- Tags: no-ordinary-database, no-fasttest, long

DROP STREAM IF EXISTS 02416_test SYNC;

CREATE STREAM 02416_test (key string, value uint32) Engine=KeeperMap('/' || current_database() || '/test2416'); -- { serverError 36 }
CREATE STREAM 02416_test (key string, value uint32) Engine=KeeperMap('/' || current_database() || '/test2416') PRIMARY KEY(key2); -- { serverError 47 }
CREATE STREAM 02416_test (key string, value uint32) Engine=KeeperMap('/' || current_database() || '/test2416') PRIMARY KEY(key, value); -- { serverError 36 }
CREATE STREAM 02416_test (key string, value uint32) Engine=KeeperMap('/' || current_database() || '/test2416') PRIMARY KEY(concat(key, value)); -- { serverError 36 }
CREATE STREAM 02416_test (key tuple(string, uint32), value uint64) Engine=KeeperMap('/' || current_database() || '/test2416') PRIMARY KEY(key);

DROP STREAM IF EXISTS 02416_test SYNC;
CREATE STREAM 02416_test (key string, value uint32) Engine=KeeperMap('/' || current_database() || '/test2416') PRIMARY KEY(key);

INSERT INTO 02416_test SELECT '1_1', number FROM numbers(1000);
SELECT count(1) == 1 FROM 02416_test;

INSERT INTO 02416_test SELECT concat(to_string(number), '_1'), number FROM numbers(1000);
SELECT count(1) == 1000 FROM 02416_test;
SELECT uniqExact(key) == 32 FROM (SELECT * FROM 02416_test LIMIT 32 SETTINGS max_block_size = 1);
SELECT sum(value) == 1 + 99 + 900 FROM 02416_test WHERE key IN ('1_1', '99_1', '900_1');

DROP STREAM IF EXISTS 02416_test SYNC;
DROP STREAM IF EXISTS 02416_test_memory;

CREATE STREAM 02416_test (k uint32, value uint64, dummy tuple(uint32, float64), bm aggregate_function(groupBitmap, uint64)) Engine=KeeperMap('/' || current_database() || '/test2416') PRIMARY KEY(k);
CREATE STREAM 02416_test_memory AS 02416_test Engine = Memory;

INSERT INTO 02416_test SELECT number % 77 AS k, sum(number) AS value, (1, 1.2), bitmapBuild(group_array(number)) FROM numbers(10000) group by k;

INSERT INTO 02416_test_memory SELECT number % 77 AS k, sum(number) AS value, (1, 1.2), bitmapBuild(group_array(number)) FROM numbers(10000) group by k;

SELECT  A.a = B.a, A.b = B.b, A.c = B.c, A.d = B.d, A.e = B.e FROM ( SELECT 0 AS a, groupBitmapMerge(bm) AS b , sum(k) AS c, sum(value) AS d, sum(dummy.1) AS e FROM 02416_test) A  ANY LEFT JOIN  (SELECT 0 AS a, groupBitmapMerge(bm) AS b , sum(k) AS c, sum(value) AS d, sum(dummy.1) AS e FROM 02416_test_memory) B USING a ORDER BY a;

TRUNCATE STREAM 02416_test;
SELECT 0 == count(1) FROM 02416_test;

DROP STREAM IF EXISTS 02416_test SYNC;
DROP STREAM IF EXISTS 02416_test_memory;
