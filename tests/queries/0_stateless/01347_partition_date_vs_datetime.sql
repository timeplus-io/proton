DROP STREAM IF EXISTS test_datetime;
create stream test_datetime (time datetime) ENGINE=MergeTree PARTITION BY time ORDER BY time;
INSERT INTO test_datetime (time) VALUES (to_date(18012));
SELECT * FROM test_datetime WHERE time=to_date(18012);
DROP STREAM test_datetime;
