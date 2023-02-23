-- Tags: no-ordinary-database, no-fasttest

DROP STREAM IF EXISTS 02418_test SYNC;

CREATE STREAM 02418_test (key uint64, value float64) Engine=KeeperMap('/' || current_database() || '/test2418', 3) PRIMARY KEY(key);

INSERT INTO 02418_test VALUES (1, 1.1), (2, 2.2);
SELECT count() FROM 02418_test;

INSERT INTO 02418_test VALUES (3, 3.3), (4, 4.4); -- { serverError 290 }

INSERT INTO 02418_test VALUES (1, 2.1), (2, 3.2), (3, 3.3);
SELECT count() FROM 02418_test;

CREATE STREAM 02418_test_another (key uint64, value float64) Engine=KeeperMap('/' || current_database() || '/test2418', 4) PRIMARY KEY(key);
INSERT INTO 02418_test VALUES (4, 4.4); -- { serverError 290 }
INSERT INTO 02418_test_another VALUES (4, 4.4);

SELECT count() FROM 02418_test;
SELECT count() FROM 02418_test_another;

DROP STREAM 02418_test SYNC;
DROP STREAM 02418_test_another SYNC;
