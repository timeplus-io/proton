SELECT 'Various intervals';

SELECT dateDiff('year', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT dateDiff('year', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT dateDiff('year', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT dateDiff('quarter', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT dateDiff('quarter', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT dateDiff('quarter', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT dateDiff('month', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT dateDiff('month', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT dateDiff('month', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT dateDiff('week', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT dateDiff('week', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT dateDiff('week', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT dateDiff('day', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT dateDiff('day', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT dateDiff('day', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT dateDiff('hour', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT dateDiff('hour', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT dateDiff('hour', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');
SELECT dateDiff('minute', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT dateDiff('minute', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT dateDiff('minute', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');
SELECT dateDiff('second', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT dateDiff('second', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT dateDiff('second', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');

SELECT 'date and DateTime arguments';

SELECT dateDiff('second', to_date('2017-12-31'), to_datetime('2016-01-01 00:00:00', 'UTC'), 'UTC');
SELECT dateDiff('second', to_datetime('2017-12-31 00:00:00', 'UTC'), to_date('2017-01-01'), 'UTC');
SELECT dateDiff('second', to_datetime('2017-12-31 00:00:00', 'UTC'), to_datetime('2018-01-01 00:00:00', 'UTC'));

SELECT 'Constant and non-constant arguments';

SELECT dateDiff('minute', materialize(to_date('2017-12-31')), to_date('2016-01-01'), 'UTC');
SELECT dateDiff('minute', to_date('2017-12-31'), materialize(to_date('2017-01-01')), 'UTC');
SELECT dateDiff('minute', materialize(to_date('2017-12-31')), materialize(to_date('2018-01-01')), 'UTC');

SELECT 'Case insensitive';

SELECT DATEDIFF('year', today(), today() - INTERVAL 10 YEAR);

SELECT 'Dependance of timezones';

SELECT dateDiff('month', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('week', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('day', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('hour', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('minute', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT dateDiff('second', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');

SELECT dateDiff('month', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT dateDiff('week', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT dateDiff('day', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT dateDiff('hour', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT dateDiff('minute', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT dateDiff('second', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');

SELECT dateDiff('month', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('week', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('day', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('hour', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('minute', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT dateDiff('second', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));

SELECT dateDiff('month', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('week', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('day', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('hour', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('minute', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT dateDiff('second', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));

SELECT 'Additional test';

SELECT number = dateDiff('month', now() - INTERVAL number MONTH, now()) FROM system.numbers LIMIT 10;
