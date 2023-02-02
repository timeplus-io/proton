DROP STREAM IF EXISTS test;
CREATE STREAM test(test string DEFAULT 'test', test_tmp int DEFAULT 1)ENGINE = Memory;
DROP STREAM test;
