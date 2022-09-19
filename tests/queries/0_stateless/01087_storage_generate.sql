DROP STREAM IF EXISTS test_table;
create stream test_table(a array(int8), d Decimal32(4), c tuple(DateTime64(3), UUID)) ENGINE=GenerateRandom();
SELECT COUNT(*) FROM (SELECT * FROM test_table LIMIT 100);

DROP STREAM IF EXISTS test_table;

SELECT '-';

DROP STREAM IF EXISTS test_table_2;
create stream test_table_2(a array(int8), d Decimal32(4), c tuple(DateTime64(3, 'Europe/Moscow'), UUID)) ENGINE=GenerateRandom(10, 5, 3);

SELECT * FROM test_table_2 LIMIT 100;

SELECT '-';

DROP STREAM IF EXISTS test_table_2;

