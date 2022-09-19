DROP STREAM IF EXISTS join_test;
create stream join_test (number uint8, value Float32) Engine = Join(ANY, LEFT, number);
TRUNCATE TABLE join_test;
DROP STREAM IF EXISTS join_test;
