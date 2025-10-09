-- SET enable_analyzer=1;
SET asterisk_include_reserved_columns=false;
SET query_mode='table';
DROP STREAM IF EXISTS test;
CREATE STREAM test (a int64, b int64, c int64) ORDER BY a;
select sleep(2) FORMAT Null;
INSERT INTO test(a, b, c) VALUES (0, 5, 10), (7, 8, 15), (14, 10, 20);
select sleep(2) FORMAT Null;
SELECT 'test-1';
SELECT *, 'original' AS orig FROM test ORDER BY a, b WITH FILL TO 20 STEP 2 STALENESS 3, c WITH FILL TO 25 step 3;

DROP STREAM IF EXISTS test2;
CREATE STREAM test2 (a int64, b int64) ORDER BY a;
select sleep(2) FORMAT Null;
INSERT INTO test2(a, b) values (1, 0), (1, 4), (1, 8), (1, 12);
select sleep(2) FORMAT Null;

SELECT 'test-2-1';
SELECT *, 'original' AS orig FROM test2 ORDER BY a, b WITH FILL;

SELECT 'test-2-2';
SELECT *, 'original' AS orig FROM test2 ORDER BY a WITH FILL to 20 STALENESS 4, b WITH FILL TO 15 STALENESS 7;

DROP STREAM IF EXISTS test3;
CREATE STREAM test3 (a int64, b int64) ORDER BY a;
select sleep(2) FORMAT Null;
INSERT INTO test3(a, b) VALUES (25, 17), (30, 18);
select sleep(2) FORMAT Null;

SELECT 'test-3-1';
SELECT a, b, 'original' AS orig FROM test3 ORDER BY a WITH FILL TO 33 STEP 3, b WITH FILL FROM -10 STEP 2;
