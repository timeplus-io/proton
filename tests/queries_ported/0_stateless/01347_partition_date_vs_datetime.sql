DROP STREAM IF EXISTS test_datetime;
CREATE STREAM test_datetime (time DateTime) ENGINE=MergeTree PARTITION BY time ORDER BY time;
INSERT INTO test_datetime (time) VALUES (to_date16(18012));
SELECT * FROM test_datetime WHERE time=to_date16(18012);
DROP STREAM test_datetime;
