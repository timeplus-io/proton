DROP STREAM IF EXISTS test_alias;

CREATE STREAM test_alias (a uint8 ALIAS b, b uint8) ENGINE Log;

SELECT count() FROM test_alias;

DROP STREAM test_alias;
