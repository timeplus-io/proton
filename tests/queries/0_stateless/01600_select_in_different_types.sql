SELECT 1 IN (SELECT 1);
SELECT -1 IN (SELECT 1);

DROP STREAM IF EXISTS select_in_test;

create stream select_in_test(value uint8) ;
INSERT INTO select_in_test VALUES (1), (2), (3);

SELECT value FROM select_in_test WHERE value IN (-1);
SELECT value FROM select_in_test WHERE value IN (SELECT -1);

SELECT value FROM select_in_test WHERE value IN (1);
SELECT value FROM select_in_test WHERE value IN (SELECT 1);

DROP STREAM select_in_test;

create stream select_in_test(value int8) ;
INSERT INTO select_in_test VALUES (-1), (2), (3);

SELECT value FROM select_in_test WHERE value IN (1);
SELECT value FROM select_in_test WHERE value IN (SELECT 1);

SELECT value FROM select_in_test WHERE value IN (2);
SELECT value FROM select_in_test WHERE value IN (SELECT 2);

DROP STREAM select_in_test;

SELECT 1 IN (1);
SELECT '1' IN (SELECT 1);

SELECT 1 IN (SELECT 1) SETTINGS transform_null_in = 1;
SELECT 1 IN (SELECT 'a') SETTINGS transform_null_in = 1;
SELECT 'a' IN (SELECT 1) SETTINGS transform_null_in = 1; -- { serverError 6 }
SELECT 1 IN (SELECT -1) SETTINGS transform_null_in = 1;
SELECT -1 IN (SELECT 1) SETTINGS transform_null_in = 1; -- { serverError 70 }
