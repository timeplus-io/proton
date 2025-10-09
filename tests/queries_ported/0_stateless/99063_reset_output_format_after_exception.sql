DROP STREAM IF EXISTS test_99063;
CREATE STREAM test_99063(j json) ENGINE=Memory;
INSERT INTO test_99063 VALUES('{"a":1,"b":"hello","c":[1,2,3]}');

SELECT j FROM test_99063 FORMAT Arrow; -- {clientError UNKNOWN_TYPE }

-- Should not have exception
SELECT j FROM test_99063;
