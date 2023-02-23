-- Tags: no-ordinary-database, no-fasttest

DROP STREAM IF EXISTS 02417_test SYNC;

CREATE STREAM 02417_test (key uint64, value uint64) Engine=KeeperMap('/' || current_database() || '/test2417') PRIMARY KEY(key);
INSERT INTO 02417_test VALUES (1, 11);
SELECT * FROM 02417_test ORDER BY key;
SELECT '------';

CREATE STREAM 02417_test_another (key uint64, value uint64) Engine=KeeperMap('/' || current_database() || '/test2417') PRIMARY KEY(key);
INSERT INTO 02417_test_another VALUES (2, 22);
SELECT * FROM 02417_test_another ORDER BY key;
SELECT '------';
SELECT * FROM 02417_test ORDER BY key;
SELECT '------';

DROP STREAM 02417_test SYNC;
SELECT * FROM 02417_test_another ORDER BY key;

DROP STREAM 02417_test_another SYNC;
