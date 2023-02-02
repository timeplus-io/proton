-- Tags: no-fasttest

DROP STREAM IF EXISTS test_collate;
DROP STREAM IF EXISTS test_collate_null;

CREATE STREAM test_collate (x uint32, s low_cardinality(string)) ENGINE=Memory();
CREATE STREAM test_collate_null (x uint32, s low_cardinality(nullable(string))) ENGINE=Memory();

INSERT INTO test_collate VALUES (1, 'Ё'), (1, 'ё'), (1, 'а'), (2, 'А'), (2, 'я'), (2, 'Я');
INSERT INTO test_collate_null VALUES (1, 'Ё'), (1, 'ё'), (1, 'а'), (2, 'А'), (2, 'я'), (2, 'Я'), (1, null), (2, null);


SELECT 'Order by without collate';
SELECT * FROM test_collate ORDER BY s;
SELECT 'Order by with collate';
SELECT * FROM test_collate ORDER BY s COLLATE 'ru';

SELECT 'Order by tuple without collate';
SELECT * FROM test_collate ORDER BY x, s;
SELECT 'Order by tuple with collate';
SELECT * FROM test_collate ORDER BY x, s COLLATE 'ru';

SELECT 'Order by without collate';
SELECT * FROM test_collate_null ORDER BY s;
SELECT 'Order by with collate';
SELECT * FROM test_collate_null ORDER BY s COLLATE 'ru';

SELECT 'Order by tuple without collate';
SELECT * FROM test_collate_null ORDER BY x, s;
SELECT 'Order by tuple with collate';
SELECT * FROM test_collate_null ORDER BY x, s COLLATE 'ru';


DROP STREAM test_collate;
DROP STREAM test_collate_null;
