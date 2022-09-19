-- Tags: no-fasttest

DROP STREAM IF EXISTS collate_test1;
DROP STREAM IF EXISTS collate_test2;
DROP STREAM IF EXISTS collate_test3;

create stream collate_test1 (x uint32, s tuple(uint32, string)) ENGINE=Memory();
create stream collate_test2 (x uint32, s tuple(uint32, LowCardinality(Nullable(string)))) ENGINE=Memory();
create stream collate_test3 (x uint32, s tuple(uint32, tuple(uint32, array(string)))) ENGINE=Memory();

INSERT INTO collate_test1 VALUES (1, (1, 'Ё')), (1, (1, 'ё')), (1, (1, 'а')), (2, (2, 'А')), (2, (1, 'я')), (2, (2, 'Я')), (1, (2,'а')), (1, (3, 'я'));
INSERT INTO collate_test2 VALUES (1, (1, 'Ё')), (1, (1, 'ё')), (1, (1, 'а')), (2, (2, 'А')), (2, (1, 'я')), (2, (2, 'Я')), (1, (2, null)), (1, (3, 'я')), (1, (1, null)), (2, (2, null));
INSERT INTO collate_test3 VALUES (1, (1, (1, ['Ё']))), (1, (2, (1, ['ё']))), (1, (1, (2, ['а']))), (2, (1, (1, ['А']))), (2, (2, (1, ['я']))), (2, (1, (1, ['Я']))), (1, (2, (1, ['ё','а']))), (1, (1, (2, ['ё', 'я']))), (2, (1, (1, ['ё', 'а', 'а'])));

SELECT * FROM collate_test1 ORDER BY s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test1 ORDER BY x, s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test2 ORDER BY s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test2 ORDER BY x, s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test3 ORDER BY s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test3 ORDER BY x, s COLLATE 'ru';
SELECT '';

DROP STREAM collate_test1;
DROP STREAM collate_test2;
DROP STREAM collate_test3;

