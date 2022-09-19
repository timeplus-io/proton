DROP STREAM IF EXISTS test;
create stream test(test string DEFAULT 'test', test_tmp int DEFAULT 1);
DROP STREAM test;
