-- Tags: no-parallel

DROP DATABASE IF EXISTS test_buffer;
CREATE DATABASE test_buffer;
create stream test_buffer.mt (uid uint64, ts DateTime, val float64) ENGINE = MergeTree PARTITION BY to_date(ts) ORDER BY (uid, ts);
create stream test_buffer.buf as test_buffer.mt ENGINE = Buffer(test_buffer, mt, 2, 10, 60, 10000, 100000, 1000000, 10000000);
INSERT INTO test_buffer.buf VALUES (1, '2019-03-01 10:00:00', 0.5), (2, '2019-03-02 10:00:00', 0.15), (1, '2019-03-03 10:00:00', 0.25);
SELECT count() from test_buffer.buf prewhere ts > to_datetime('2019-03-01 12:00:00') and ts < to_datetime('2019-03-02 12:00:00');
DROP DATABASE test_buffer;
