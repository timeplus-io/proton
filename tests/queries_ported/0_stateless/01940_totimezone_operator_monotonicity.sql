DROP STREAM IF EXISTS totimezone_op_mono;
CREATE STREAM totimezone_op_mono(i int, tz string, create_time DateTime) ENGINE MergeTree PARTITION BY to_date(create_time) ORDER BY i;
INSERT INTO totimezone_op_mono VALUES (1, 'UTC', to_datetime('2020-09-01 00:00:00', 'UTC')), (2, 'UTC', to_datetime('2020-09-02 00:00:00', 'UTC'));
SET max_rows_to_read = 1;
SELECT count() FROM totimezone_op_mono WHERE to_timezone(create_time, 'UTC') = '2020-09-01 00:00:00';
DROP STREAM IF EXISTS totimezone_op_mono;
