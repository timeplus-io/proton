DROP STREAM IF EXISTS join_test;
create stream join_test (number uint8, value float32) Engine = Join(ANY, LEFT, number);
TRUNCATE STREAM join_test;
DROP STREAM IF EXISTS join_test;
