-- { echo }
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 SECOND, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 MINUTE, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' HOUR, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 WEEK, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' MONTH, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' QUARTER, 'US/Samoa');
SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' YEAR, 'US/Samoa');

SELECT tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT tumbleStart(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT to_datetime(tumbleStart(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT to_datetime(tumbleStart(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT tumbleStart(tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'));
SELECT tumbleEnd(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa');
SELECT to_datetime(tumbleEnd(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT to_datetime(tumbleEnd(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'), 'US/Samoa');
SELECT tumbleEnd(tumble(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, 'US/Samoa'));

SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 SECOND, INTERVAL 3 SECOND, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 MINUTE, INTERVAL 3 MINUTE, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 HOUR, INTERVAL 3 HOUR, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 DAY, INTERVAL 3 DAY, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 WEEK, INTERVAL 3 WEEK, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 MONTH, INTERVAL 3 MONTH, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 QUARTER, INTERVAL 3 QUARTER, 'US/Samoa');
SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL 1 YEAR, INTERVAL 3 YEAR, 'US/Samoa');

SELECT hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa');
SELECT hopStart(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa');
SELECT to_datetime(hopStart(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT to_datetime(hopStart(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT hopStart(hop(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'));
SELECT hopEnd(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa');
SELECT to_datetime(hopEnd(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT to_datetime(hopEnd(to_datetime('2020-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'), 'US/Samoa');
SELECT hopEnd(hop(to_datetime('2019-01-09 12:00:01', 'US/Samoa'), INTERVAL '1' DAY, INTERVAL '3' DAY, 'US/Samoa'));
