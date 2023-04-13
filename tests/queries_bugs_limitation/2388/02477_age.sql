SELECT 'Various intervals';

SELECT age('year', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT age('year', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT age('year', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT age('quarter', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT age('quarter', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT age('quarter', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT age('month', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT age('month', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT age('month', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT age('week', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT age('week', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT age('week', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT age('day', to_date('2017-12-31'), to_date('2016-01-01'));
SELECT age('day', to_date('2017-12-31'), to_date('2017-01-01'));
SELECT age('day', to_date('2017-12-31'), to_date('2018-01-01'));
SELECT age('hour', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT age('hour', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT age('hour', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');
SELECT age('minute', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT age('minute', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT age('minute', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');
SELECT age('second', to_date('2017-12-31'), to_date('2016-01-01'), 'UTC');
SELECT age('second', to_date('2017-12-31'), to_date('2017-01-01'), 'UTC');
SELECT age('second', to_date('2017-12-31'), to_date('2018-01-01'), 'UTC');

SELECT 'DateTime arguments';
SELECT age('day', to_datetime('2016-01-01 00:00:01', 'UTC'), to_datetime('2016-01-02 00:00:00', 'UTC'), 'UTC');
SELECT age('hour', to_datetime('2016-01-01 00:00:01', 'UTC'), to_datetime('2016-01-02 00:00:00', 'UTC'), 'UTC');
SELECT age('minute', to_datetime('2016-01-01 00:00:01', 'UTC'), to_datetime('2016-01-02 00:00:00', 'UTC'), 'UTC');
SELECT age('second', to_datetime('2016-01-01 00:00:01', 'UTC'), to_datetime('2016-01-02 00:00:00', 'UTC'), 'UTC');

SELECT 'Date and DateTime arguments';

SELECT age('second', to_date('2017-12-31'), to_datetime('2016-01-01 00:00:00', 'UTC'), 'UTC');
SELECT age('second', to_datetime('2017-12-31 00:00:00', 'UTC'), to_date('2017-01-01'), 'UTC');
SELECT age('second', to_datetime('2017-12-31 00:00:00', 'UTC'), to_datetime('2018-01-01 00:00:00', 'UTC'));

SELECT 'Constant and non-constant arguments';

SELECT age('minute', materialize(to_date('2017-12-31')), to_date('2016-01-01'), 'UTC');
SELECT age('minute', to_date('2017-12-31'), materialize(to_date('2017-01-01')), 'UTC');
SELECT age('minute', materialize(to_date('2017-12-31')), materialize(to_date('2018-01-01')), 'UTC');

SELECT 'Case insensitive';

SELECT age('year', today(), today() - INTERVAL 10 YEAR);

SELECT 'Dependance of timezones';

SELECT age('month', to_date('2014-10-26'), to_date('2014-10-27'), 'Asia/Istanbul');
SELECT age('week', to_date('2014-10-26'), to_date('2014-10-27'), 'Asia/Istanbul');
SELECT age('day', to_date('2014-10-26'), to_date('2014-10-27'), 'Asia/Istanbul');
SELECT age('hour', to_date('2014-10-26'), to_date('2014-10-27'), 'Asia/Istanbul');
SELECT age('minute', to_date('2014-10-26'), to_date('2014-10-27'), 'Asia/Istanbul');
SELECT age('second', to_date('2014-10-26'), to_date('2014-10-27'), 'Asia/Istanbul');

SELECT age('month', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT age('week', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT age('day', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT age('hour', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT age('minute', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');
SELECT age('second', to_date('2014-10-26'), to_date('2014-10-27'), 'UTC');

SELECT age('month', to_datetime('2014-10-26 00:00:00', 'Asia/Istanbul'), to_datetime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('week', to_datetime('2014-10-26 00:00:00', 'Asia/Istanbul'), to_datetime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('day', to_datetime('2014-10-26 00:00:00', 'Asia/Istanbul'), to_datetime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('hour', to_datetime('2014-10-26 00:00:00', 'Asia/Istanbul'), to_datetime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('minute', to_datetime('2014-10-26 00:00:00', 'Asia/Istanbul'), to_datetime('2014-10-27 00:00:00', 'Asia/Istanbul'));
SELECT age('second', to_datetime('2014-10-26 00:00:00', 'Asia/Istanbul'), to_datetime('2014-10-27 00:00:00', 'Asia/Istanbul'));

SELECT age('month', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT age('week', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT age('day', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT age('hour', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT age('minute', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));
SELECT age('second', to_datetime('2014-10-26 00:00:00', 'UTC'), to_datetime('2014-10-27 00:00:00', 'UTC'));

SELECT 'Additional test';

SELECT number = age('month', now() - INTERVAL number MONTH, now()) FROM system.numbers LIMIT 10;
