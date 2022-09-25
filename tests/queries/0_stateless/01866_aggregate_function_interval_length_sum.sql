DROP STREAM IF EXISTS interval;
DROP STREAM IF EXISTS fl_interval;
DROP STREAM IF EXISTS dt_interval;
DROP STREAM IF EXISTS date_interval;

create stream interval ( `id` string, `start` int64, `end` int64 ) ENGINE = MergeTree ORDER BY start;
INSERT INTO interval VALUES ('a', 1, 3), ('a', 1, 3), ('a', 2, 4), ('a', 1, 1), ('a', 5, 6), ('a', 5, 7), ('b', 10, 12), ('b', 13, 19), ('b', 14, 16), ('c', -1, 1), ('c', -2, -1);

create stream fl_interval ( `id` string, `start` Float, `end` Float ) ENGINE = MergeTree ORDER BY start;
INSERT INTO fl_interval VALUES ('a', 1.1, 3.2), ('a', 1.5, 3.6), ('a', 4.0, 5.0);

create stream dt_interval ( `id` string, `start` datetime, `end` datetime ) ENGINE = MergeTree ORDER BY start;
INSERT INTO dt_interval VALUES ('a', '2020-01-01 02:11:22', '2020-01-01 03:12:31'), ('a', '2020-01-01 01:12:30', '2020-01-01 02:50:11');

create stream date_interval ( `id` string, `start` date, `end` date ) ENGINE = MergeTree ORDER BY start;
INSERT INTO date_interval VALUES ('a', '2020-01-01', '2020-01-04'), ('a', '2020-01-03', '2020-01-08 02:50:11');

SELECT id, intervalLengthSum(start, end), to_type_name(intervalLengthSum(start, end)) FROM interval GROUP BY id ORDER BY id;
SELECT id, 3.4 < intervalLengthSum(start, end) AND intervalLengthSum(start, end) < 3.6, to_type_name(intervalLengthSum(start, end)) FROM fl_interval GROUP BY id ORDER BY id;
SELECT id, intervalLengthSum(start, end), to_type_name(intervalLengthSum(start, end)) FROM dt_interval GROUP BY id ORDER BY id;
SELECT id, intervalLengthSum(start, end), to_type_name(intervalLengthSum(start, end)) FROM date_interval GROUP BY id ORDER BY id;

DROP STREAM interval;
DROP STREAM fl_interval;
DROP STREAM dt_interval;
DROP STREAM date_interval;
