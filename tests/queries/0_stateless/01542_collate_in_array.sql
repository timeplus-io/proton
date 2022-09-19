-- Tags: no-fasttest

DROP STREAM IF EXISTS collate_test1;
DROP STREAM IF EXISTS collate_test2;
DROP STREAM IF EXISTS collate_test3;

create stream collate_test1 (x uint32, s array(string)) ENGINE=Memory();
create stream collate_test2 (x uint32, s array(LowCardinality(Nullable(string)))) ENGINE=Memory();
create stream collate_test3 (x uint32, s array(array(string))) ENGINE=Memory();

INSERT INTO collate_test1 VALUES (1, ['Ё']), (1, ['ё']), (1, ['а']), (2, ['А']), (2, ['я', 'а']), (2, ['Я']), (1, ['ё','а']), (1, ['ё', 'я']), (2, ['ё', 'а', 'а']);
INSERT INTO collate_test2 VALUES (1, ['Ё']), (1, ['ё']), (1, ['а']), (2, ['А']), (2, ['я']), (2, [null, 'Я']), (1, ['ё','а']), (1, ['ё', null, 'я']), (2, ['ё', 'а', 'а', null]);
INSERT INTO collate_test3 VALUES (1, [['а', 'я'], ['а', 'ё']]), (1, [['а', 'Ё'], ['ё', 'я']]), (2, [['ё']]), (2, [['а', 'а'], ['я', 'ё']]);

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

