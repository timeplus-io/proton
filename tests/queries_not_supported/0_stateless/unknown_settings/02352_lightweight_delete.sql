DROP STREAM IF EXISTS lwd_test;

CREATE STREAM lwd_test (id uint64 , value string) ENGINE MergeTree() ORDER BY id;

INSERT INTO lwd_test SELECT number, randomString(10) FROM system.numbers LIMIT 1000000;

SET mutations_sync = 0;
SET allow_experimental_lightweight_delete = 1;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Delete 100K rows using lightweight DELETE';
--ALTER STREAM lwd_test UPDATE _row_exists = 0 WHERE id < 3000000;
DELETE FROM lwd_test WHERE id < 100000;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Force merge to cleanup deleted rows';
OPTIMIZE TABLE lwd_test FINAL SETTINGS mutations_sync = 2;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Delete 100K more rows using lightweight DELETE';
DELETE FROM lwd_test WHERE id < 200000;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Do UPDATE mutation';
ALTER STREAM lwd_test UPDATE value = 'v' WHERE id % 2 == 0 SETTINGS mutations_sync = 2;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Force merge to cleanup deleted rows';
OPTIMIZE TABLE lwd_test FINAL SETTINGS mutations_sync = 2;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Delete 100K more rows using lightweight DELETE';
DELETE FROM lwd_test WHERE id < 300000;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Do ALTER DELETE mutation that does a "heavyweight" delete';
ALTER STREAM lwd_test DELETE WHERE id % 3 == 0 SETTINGS mutations_sync = 2;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;

SELECT 'Delete 100K more rows using lightweight DELETE';
DELETE FROM lwd_test WHERE id >= 300000 and id < 400000;


SELECT 'Force merge to cleanup deleted rows';
OPTIMIZE TABLE lwd_test FINAL SETTINGS mutations_sync = 2;

SELECT 'Rows in parts', sum(rows) FROM system.parts WHERE database = current_database() AND stream = 'lwd_test' AND active;
SELECT 'Count', count() FROM lwd_test;
SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


DROP STREAM lwd_test;
