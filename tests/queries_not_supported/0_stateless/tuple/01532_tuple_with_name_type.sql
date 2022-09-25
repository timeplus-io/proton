DROP STREAM IF EXISTS test_01532_1;
DROP STREAM IF EXISTS test_01532_2;
DROP STREAM IF EXISTS test_01532_3;
DROP STREAM IF EXISTS test_01532_4;

create stream test_01532_1 (a tuple(key string, value string)) ENGINE Memory();
DESCRIBE TABLE test_01532_1;

create stream test_01532_2 (a tuple(tuple(key string, value string))) ENGINE Memory();
DESCRIBE TABLE test_01532_2;

create stream test_01532_3 (a array(tuple(key string, value string))) ENGINE Memory();
DESCRIBE TABLE test_01532_3;

create stream test_01532_4 (a tuple(uint8, tuple(key string, value string))) ENGINE Memory();
DESCRIBE TABLE test_01532_4;

DROP STREAM test_01532_1;
DROP STREAM test_01532_2;
DROP STREAM test_01532_3;
DROP STREAM test_01532_4;
