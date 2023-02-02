DROP STREAM IF EXISTS test_datetime;

CREATE STREAM test_datetime(timestamp DateTime('Asia/Istanbul')) ENGINE=Log;

INSERT INTO test_datetime VALUES ('2020-10-15 00:00:00');

SET date_time_output_format = 'simple';
SELECT timestamp FROM test_datetime;
SELECT formatDateTime(to_datetime('2020-10-15 00:00:00', 'Asia/Istanbul'), '%Y-%m-%d %R:%S') as formatted_simple FROM test_datetime;

SET date_time_output_format = 'iso';
SELECT timestamp FROM test_datetime;
SELECT formatDateTime(to_datetime('2020-10-15 00:00:00', 'Asia/Istanbul'), '%Y-%m-%dT%R:%SZ', 'UTC') as formatted_iso FROM test_datetime;;

SET date_time_output_format = 'unix_timestamp';
SELECT timestamp FROM test_datetime;
SELECT toUnixTimestamp(timestamp) FROM test_datetime;

SET date_time_output_format = 'simple';
DROP STREAM test_datetime;

CREATE STREAM test_datetime(timestamp DateTime64(3, 'Asia/Istanbul')) Engine=Log;

INSERT INTO test_datetime VALUES ('2020-10-15 00:00:00'), (1602709200123);

SET date_time_output_format = 'simple';
SELECT timestamp FROM test_datetime;

SET date_time_output_format = 'iso';
SELECT timestamp FROM test_datetime;

SET date_time_output_format = 'unix_timestamp';
SELECT timestamp FROM test_datetime;

SET date_time_output_format = 'simple';
DROP STREAM test_datetime;
