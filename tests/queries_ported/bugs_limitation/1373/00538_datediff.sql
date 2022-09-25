SELECT 'Various intervals';

SELECT date_diff('year', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT date_diff('year', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT date_diff('year', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT date_diff('quarter', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT date_diff('quarter', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT date_diff('quarter', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT date_diff('month', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT date_diff('month', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT date_diff('month', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT date_diff('week', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT date_diff('week', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT date_diff('week', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT date_diff('day', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT date_diff('day', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT date_diff('day', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT date_diff('hour', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT date_diff('hour', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT date_diff('hour', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');
SELECT date_diff('minute', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT date_diff('minute', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT date_diff('minute', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');
SELECT date_diff('second', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT date_diff('second', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT date_diff('second', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');

SELECT 'date and datetime arguments';

SELECT date_diff('second', to_date('2017-12-31'), to_datetime('2016-01-01 00:00:00', 'UTC'), 'UTC');
SELECT date_diff('second', to_datetime('2017-12-31 00:00:00', 'UTC'), to_date('2017-01-01'), 'UTC');
SELECT date_diff('second', to_datetime('2017-12-31 00:00:00', 'UTC'), to_datetime('2018-01-01 00:00:00', 'UTC'));

SELECT 'Constant and non-constant arguments';

SELECT date_diff('minute', materialize(to_date('2017-12-31')), to_date('2016-01-01'), 'UTC');
SELECT date_diff('minute', to_date('2017-12-31'), materialize(to_date('2017-01-01')), 'UTC');
SELECT date_diff('minute', materialize(to_date('2017-12-31')), materialize(to_date('2018-01-01')), 'UTC');

SELECT 'Case insensitive';

SELECT DATEDIFF('year', today(), today() - INTERVAL 10 YEAR);

SELECT 'Dependance of timezones';

SELECT date_diff('month', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT date_diff('week', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT date_diff('day', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT date_diff('hour', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT date_diff('minute', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');
SELECT date_diff('second', to_date('2014-10-26'), to_date('2014-10-27'), 'Europe/Moscow');

SELECT date_diff('month', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT date_diff('week', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT date_diff('day', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT date_diff('hour', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT date_diff('minute', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT date_diff('second', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');

SELECT date_diff('month', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT date_diff('week', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT date_diff('day', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT date_diff('hour', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT date_diff('minute', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));
SELECT date_diff('second', to_datetime('2014-10-26 00:00:00', 'Europe/Moscow'), to_datetime('2014-10-27 00:00:00', 'Europe/Moscow'));

SELECT date_diff('month', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT date_diff('week', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT date_diff('day', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT date_diff('hour', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT date_diff('minute', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT date_diff('second', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));

SELECT 'Additional test';

SELECT number = date_diff('month', now() - INTERVAL number MONTH, now()) FROM system.numbers LIMIT 10;
