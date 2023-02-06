DROP STREAM IF EXISTS interval;
DROP STREAM IF EXISTS fl_interval;
DROP STREAM IF EXISTS dt_interval;
DROP STREAM IF EXISTS date_interval;

CREATE STREAM interval ( `id` string, `start` int64, `end` int64 ) ENGINE = MergeTree ORDER BY start;
INSERT INTO interval VALUES ('a', 1, 3), ('a', 1, 3), ('a', 2, 4), ('a', 1, 1), ('a', 5, 6), ('a', 5, 7), ('b', 10, 12), ('b', 13, 19), ('b', 14, 16), ('c', -1, 1), ('c', -2, -1);

CREATE STREAM fl_interval ( `id` string, `start` float, `end` float ) ENGINE = MergeTree ORDER BY start;
INSERT INTO fl_interval VALUES ('a', 1.1, 3.2), ('a', 1.5, 3.6), ('a', 4.0, 5.0);

CREATE STREAM dt_interval ( `id` string, `start` DateTime, `end` DateTime ) ENGINE = MergeTree ORDER BY start;
INSERT INTO dt_interval VALUES ('a', '2020-01-01 02:11:22', '2020-01-01 03:12:31'), ('a', '2020-01-01 01:12:30', '2020-01-01 02:50:11');

CREATE STREAM date_interval ( `id` string, `start` Date, `end` Date ) ENGINE = MergeTree ORDER BY start;
INSERT INTO date_interval VALUES ('a', '2020-01-01', '2020-01-04'), ('a', '2020-01-03', '2020-01-08 02:50:11');

SELECT id, interval_length_sum(start, end), to_type_name(interval_length_sum(start, end)) FROM interval GROUP BY id ORDER BY id;
SELECT id, 3.4 < interval_length_sum(start, end) AND interval_length_sum(start, end) < 3.6, to_type_name(interval_length_sum(start, end)) FROM fl_interval GROUP BY id ORDER BY id;
SELECT id, interval_length_sum(start, end), to_type_name(interval_length_sum(start, end)) FROM dt_interval GROUP BY id ORDER BY id;
SELECT id, interval_length_sum(start, end), to_type_name(interval_length_sum(start, end)) FROM date_interval GROUP BY id ORDER BY id;

DROP STREAM interval;
DROP STREAM fl_interval;
DROP STREAM dt_interval;
DROP STREAM date_interval;
