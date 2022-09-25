-- Tags: no-fasttest

DROP STREAM IF EXISTS test_collate;

create stream test_collate (x uint32, s nullable(string)) ENGINE=Memory();

INSERT INTO test_collate VALUES (1, 'Ё'), (1, 'ё'), (1, 'а'), (1, null), (2, 'А'), (2, 'я'), (2, 'Я'), (2, null);

SELECT 'Order by without collate';
SELECT * FROM test_collate ORDER BY s;
SELECT 'Order by with collate';
SELECT * FROM test_collate ORDER BY s COLLATE 'ru';

SELECT 'Order by tuple without collate';
SELECT * FROM test_collate ORDER BY x, s;
SELECT 'Order by tuple with collate';
SELECT * FROM test_collate ORDER BY x, s COLLATE 'ru';

DROP STREAM test_collate;

