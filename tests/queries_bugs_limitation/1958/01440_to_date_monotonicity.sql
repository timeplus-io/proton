DROP STREAM IF EXISTS tdm;
DROP STREAM IF EXISTS tdm2;
CREATE STREAM tdm (x DateTime('Asia/Istanbul')) ENGINE = MergeTree ORDER BY x SETTINGS write_final_mark = 0;
INSERT INTO tdm VALUES (now());
SELECT count(x) FROM tdm WHERE to_date(x) < to_date(now(), 'Asia/Istanbul') SETTINGS max_rows_to_read = 1;

SELECT to_date(-1), to_date(10000000000000, 'Asia/Istanbul'), to_date(100), to_date(65536, 'UTC'), to_date(65535, 'Asia/Istanbul');
SELECT to_datetime(-1, 'Asia/Istanbul'), to_datetime(10000000000000, 'Asia/Istanbul'), to_datetime(1000, 'Asia/Istanbul');

CREATE STREAM tdm2 (timestamp uint32) ENGINE = MergeTree ORDER BY timestamp SETTINGS index_granularity = 1;

INSERT INTO tdm2 VALUES (to_unix_timestamp('2000-01-01 13:12:12')), (to_unix_timestamp('2000-01-01 14:12:12')), (to_unix_timestamp('2000-01-01 15:12:12'));

SET max_rows_to_read = 1;
SELECT to_datetime(timestamp) FROM tdm2 WHERE to_hour(to_datetime(timestamp)) = 13;

DROP STREAM tdm;
DROP STREAM tdm2;
