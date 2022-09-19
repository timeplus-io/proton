-- Tags: long

/* timestamp 1419800400 == 2014-12-29 00:00:00 (Europe/Moscow) */
/* timestamp 1412106600 == 2014-09-30 23:50:00 (Europe/Moscow) */
/* timestamp 1420102800 == 2015-01-01 12:00:00 (Europe/Moscow) */
/* timestamp 1428310800 == 2015-04-06 12:00:00 (Europe/Moscow) */
/* timestamp 1436956200 == 2015-07-15 13:30:00 (Europe/Moscow) */
/* timestamp 1426415400 == 2015-03-15 13:30:00 (Europe/Moscow) */
/* timestamp 1549483055 == 2019-02-06 22:57:35 (Europe/Moscow) */
/* date 16343 == 2014-09-30 */
/* date 16433 == 2014-12-29 */
/* date 17933 == 2019-02-06 */

/* to_start_of_day */

SELECT 'to_start_of_day';
SELECT to_start_of_day(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_start_of_day(to_datetime(1412106600), 'Europe/Paris');
SELECT to_start_of_day(to_datetime(1412106600), 'Europe/London');
SELECT to_start_of_day(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_start_of_day(to_datetime(1412106600), 'Pacific/Pitcairn');
SELECT to_start_of_day(to_date(16343), 'Europe/Moscow');
SELECT to_start_of_day(to_date(16343), 'Europe/Paris');
SELECT to_start_of_day(to_date(16343), 'Europe/London');
SELECT to_start_of_day(to_date(16343), 'Asia/Tokyo');
SELECT to_start_of_day(to_date(16343), 'Pacific/Pitcairn');

/* to_monday */

SELECT 'to_monday';
SELECT to_monday(to_datetime(1419800400), 'Europe/Moscow');
SELECT to_monday(to_datetime(1419800400), 'Europe/Paris');
SELECT to_monday(to_datetime(1419800400), 'Europe/London');
SELECT to_monday(to_datetime(1419800400), 'Asia/Tokyo');
SELECT to_monday(to_datetime(1419800400), 'Pacific/Pitcairn');
SELECT to_monday(to_date(16433));
SELECT to_monday(to_date(16433));
SELECT to_monday(to_date(16433));
SELECT to_monday(to_date(16433));
SELECT to_monday(to_date(16433));

/* toStartOfMonth */

SELECT 'toStartOfMonth';
SELECT to_start_of_month(to_datetime(1419800400), 'Europe/Moscow');
SELECT to_start_of_month(to_datetime(1419800400), 'Europe/Paris');
SELECT to_start_of_month(to_datetime(1419800400), 'Europe/London');
SELECT to_start_of_month(to_datetime(1419800400), 'Asia/Tokyo');
SELECT to_start_of_month(to_datetime(1419800400), 'Pacific/Pitcairn');
SELECT to_start_of_month(to_date(16433));
SELECT to_start_of_month(to_date(16433));
SELECT to_start_of_month(to_date(16433));
SELECT to_start_of_month(to_date(16433));
SELECT to_start_of_month(to_date(16433));

/* to_start_of_quarter */

SELECT 'to_start_of_quarter';
SELECT to_start_of_quarter(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_start_of_quarter(to_datetime(1412106600), 'Europe/Paris');
SELECT to_start_of_quarter(to_datetime(1412106600), 'Europe/London');
SELECT to_start_of_quarter(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_start_of_quarter(to_datetime(1412106600), 'Pacific/Pitcairn');
SELECT to_start_of_quarter(to_date(16343));
SELECT to_start_of_quarter(to_date(16343));
SELECT to_start_of_quarter(to_date(16343));
SELECT to_start_of_quarter(to_date(16343));
SELECT to_start_of_quarter(to_date(16343));

/* to_start_of_year */

SELECT 'to_start_of_year';
SELECT to_start_of_year(to_datetime(1419800400), 'Europe/Moscow');
SELECT to_start_of_year(to_datetime(1419800400), 'Europe/Paris');
SELECT to_start_of_year(to_datetime(1419800400), 'Europe/London');
SELECT to_start_of_year(to_datetime(1419800400), 'Asia/Tokyo');
SELECT to_start_of_year(to_datetime(1419800400), 'Pacific/Pitcairn');
SELECT to_start_of_year(to_date(16433));
SELECT to_start_of_year(to_date(16433));
SELECT to_start_of_year(to_date(16433));
SELECT to_start_of_year(to_date(16433));
SELECT to_start_of_year(to_date(16433));

/* to_time */

SELECT 'to_time';
SELECT to_string(to_time(to_datetime(1420102800), 'Europe/Moscow'), 'Europe/Moscow'), to_string(to_time(to_datetime(1428310800), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_time(to_datetime(1420102800), 'Europe/Paris'), 'Europe/Paris'), to_string(to_time(to_datetime(1428310800), 'Europe/Paris'), 'Europe/Paris');
SELECT to_string(to_time(to_datetime(1420102800), 'Europe/London'), 'Europe/London'), to_string(to_time(to_datetime(1428310800), 'Europe/London'), 'Europe/London');
SELECT to_string(to_time(to_datetime(1420102800), 'Asia/Tokyo'), 'Asia/Tokyo'), to_string(to_time(to_datetime(1428310800), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_string(to_time(to_datetime(1420102800), 'Pacific/Pitcairn'), 'Pacific/Pitcairn'), to_string(to_time(to_datetime(1428310800), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* to_year */

SELECT 'to_year';
SELECT to_year(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_year(to_datetime(1412106600), 'Europe/Paris');
SELECT to_year(to_datetime(1412106600), 'Europe/London');
SELECT to_year(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_year(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_month */

SELECT 'to_month';
SELECT to_month(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_month(to_datetime(1412106600), 'Europe/Paris');
SELECT to_month(to_datetime(1412106600), 'Europe/London');
SELECT to_month(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_month(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_day_of_month */

SELECT 'to_day_of_month';
SELECT to_day_of_month(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_day_of_month(to_datetime(1412106600), 'Europe/Paris');
SELECT to_day_of_month(to_datetime(1412106600), 'Europe/London');
SELECT to_day_of_month(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_day_of_month(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_day_of_week */

SELECT 'to_day_of_week';
SELECT to_day_of_week(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_day_of_week(to_datetime(1412106600), 'Europe/Paris');
SELECT to_day_of_week(to_datetime(1412106600), 'Europe/London');
SELECT to_day_of_week(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_day_of_week(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_hour */

SELECT 'to_hour';
SELECT to_hour(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_hour(to_datetime(1412106600), 'Europe/Paris');
SELECT to_hour(to_datetime(1412106600), 'Europe/London');
SELECT to_hour(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_hour(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_minute */

SELECT 'to_minute';
SELECT to_minute(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_minute(to_datetime(1412106600), 'Europe/Paris');
SELECT to_minute(to_datetime(1412106600), 'Europe/London');
SELECT to_minute(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_minute(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_second */

SELECT 'to_second';
SELECT to_second(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_second(to_datetime(1412106600), 'Europe/Paris');
SELECT to_second(to_datetime(1412106600), 'Europe/London');
SELECT to_second(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_second(to_datetime(1412106600), 'Pacific/Pitcairn');

/* to_start_of_minute */

SELECT 'to_start_of_minute';
SELECT to_string(to_start_of_minute(to_datetime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_minute(to_datetime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT to_string(to_start_of_minute(to_datetime(1549483055), 'Europe/London'), 'Europe/London');
SELECT to_string(to_start_of_minute(to_datetime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_string(to_start_of_minute(to_datetime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* to_start_of_five_minute */

SELECT 'to_start_of_five_minute';
SELECT to_string(to_start_of_five_minute(to_datetime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_five_minute(to_datetime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT to_string(to_start_of_five_minute(to_datetime(1549483055), 'Europe/London'), 'Europe/London');
SELECT to_string(to_start_of_five_minute(to_datetime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_string(to_start_of_five_minute(to_datetime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* to_start_of_ten_minute */

SELECT 'to_start_of_ten_minute';
SELECT to_string(to_start_of_ten_minute(to_datetime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_ten_minute(to_datetime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT to_string(to_start_of_ten_minute(to_datetime(1549483055), 'Europe/London'), 'Europe/London');
SELECT to_string(to_start_of_ten_minute(to_datetime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_string(to_start_of_ten_minute(to_datetime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* to_start_of_fifteen_minute */

SELECT 'to_start_of_fifteen_minute';
SELECT to_string(to_start_of_fifteen_minute(to_datetime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_fifteen_minute(to_datetime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT to_string(to_start_of_fifteen_minute(to_datetime(1549483055), 'Europe/London'), 'Europe/London');
SELECT to_string(to_start_of_fifteen_minute(to_datetime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_string(to_start_of_fifteen_minute(to_datetime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* to_start_of_hour */

SELECT 'to_start_of_hour';
SELECT to_string(to_start_of_hour(to_datetime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_hour(to_datetime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT to_string(to_start_of_hour(to_datetime(1549483055), 'Europe/London'), 'Europe/London');
SELECT to_string(to_start_of_hour(to_datetime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_string(to_start_of_hour(to_datetime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* to_start_of_interval */

SELECT 'to_start_of_interval';
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 1 year, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 2 year, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 5 year, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 1 quarter, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 2 quarter, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 3 quarter, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 1 month, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 2 month, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 5 month, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 1 week, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 2 week, 'Europe/Moscow');
SELECT to_start_of_interval(to_datetime(1549483055), INTERVAL 6 week, 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 1 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 2 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 5 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 1 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 2 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 6 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 24 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 1 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 2 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 5 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 20 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 90 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 1 second, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 2 second, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_datetime(1549483055), INTERVAL 5 second, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_start_of_interval(to_date(17933), INTERVAL 1 year);
SELECT to_start_of_interval(to_date(17933), INTERVAL 2 year);
SELECT to_start_of_interval(to_date(17933), INTERVAL 5 year);
SELECT to_start_of_interval(to_date(17933), INTERVAL 1 quarter);
SELECT to_start_of_interval(to_date(17933), INTERVAL 2 quarter);
SELECT to_start_of_interval(to_date(17933), INTERVAL 3 quarter);
SELECT to_start_of_interval(to_date(17933), INTERVAL 1 month);
SELECT to_start_of_interval(to_date(17933), INTERVAL 2 month);
SELECT to_start_of_interval(to_date(17933), INTERVAL 5 month);
SELECT to_start_of_interval(to_date(17933), INTERVAL 1 week);
SELECT to_start_of_interval(to_date(17933), INTERVAL 2 week);
SELECT to_start_of_interval(to_date(17933), INTERVAL 6 week);
SELECT to_string(to_start_of_interval(to_date(17933), INTERVAL 1 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_date(17933), INTERVAL 2 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_string(to_start_of_interval(to_date(17933), INTERVAL 5 day, 'Europe/Moscow'), 'Europe/Moscow');

/* to_relative_year_num */

SELECT 'to_relative_year_num';
SELECT to_relative_year_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_year_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_year_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_year_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_year_num(to_datetime(1412106600), 'Europe/London') - to_relative_year_num(to_datetime(0), 'Europe/London');
SELECT to_relative_year_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_year_num(to_datetime(0), 'Asia/Tokyo');
SELECT to_relative_year_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_year_num(to_datetime(0), 'Pacific/Pitcairn');

/* to_relative_month_num */

SELECT 'to_relative_month_num';
SELECT to_relative_month_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_month_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_month_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_month_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_month_num(to_datetime(1412106600), 'Europe/London') - to_relative_month_num(to_datetime(0), 'Europe/London');
SELECT to_relative_month_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_month_num(to_datetime(0), 'Asia/Tokyo');
SELECT to_relative_month_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_month_num(to_datetime(0), 'Pacific/Pitcairn');

/* to_relative_week_num */

SELECT 'to_relative_week_num';
SELECT to_relative_week_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_week_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_week_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_week_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_week_num(to_datetime(1412106600), 'Europe/London') - to_relative_week_num(to_datetime(0), 'Europe/London');
SELECT to_relative_week_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_week_num(to_datetime(0), 'Asia/Tokyo');
SELECT to_relative_week_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_week_num(to_datetime(0), 'Pacific/Pitcairn');

/* to_relative_day_num */

SELECT 'to_relative_day_num';
SELECT to_relative_day_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_day_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_day_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_day_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_day_num(to_datetime(1412106600), 'Europe/London') - to_relative_day_num(to_datetime(0), 'Europe/London');
SELECT to_relative_day_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_day_num(to_datetime(0), 'Asia/Tokyo');
-- NOTE: to_relative_day_num(to_datetime(0), 'Pacific/Pitcairn') overflows from -1 to 65535
SELECT to_uint16(to_relative_day_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_day_num(to_datetime(0), 'Pacific/Pitcairn'));

/* to_relative_hour_num */

SELECT 'to_relative_hour_num';
SELECT to_relative_hour_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_hour_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_hour_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_hour_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_hour_num(to_datetime(1412106600), 'Europe/London') - to_relative_hour_num(to_datetime(0), 'Europe/London');
SELECT to_relative_hour_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_hour_num(to_datetime(0), 'Asia/Tokyo');
SELECT to_relative_hour_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_hour_num(to_datetime(0), 'Pacific/Pitcairn');

/* to_relative_minute_num */

SELECT 'to_relative_minute_num';
SELECT to_relative_minute_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_minute_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_minute_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_minute_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_minute_num(to_datetime(1412106600), 'Europe/London') - to_relative_minute_num(to_datetime(0), 'Europe/London');
SELECT to_relative_minute_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_minute_num(to_datetime(0), 'Asia/Tokyo');
SELECT to_relative_minute_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_minute_num(to_datetime(0), 'Pacific/Pitcairn');

/* to_relative_second_num */

SELECT 'to_relative_second_num';
SELECT to_relative_second_num(to_datetime(1412106600), 'Europe/Moscow') - to_relative_second_num(to_datetime(0), 'Europe/Moscow');
SELECT to_relative_second_num(to_datetime(1412106600), 'Europe/Paris') - to_relative_second_num(to_datetime(0), 'Europe/Paris');
SELECT to_relative_second_num(to_datetime(1412106600), 'Europe/London') - to_relative_second_num(to_datetime(0), 'Europe/London');
SELECT to_relative_second_num(to_datetime(1412106600), 'Asia/Tokyo') - to_relative_second_num(to_datetime(0), 'Asia/Tokyo');
SELECT to_relative_second_num(to_datetime(1412106600), 'Pacific/Pitcairn') - to_relative_second_num(to_datetime(0), 'Pacific/Pitcairn');

/* toDate */

SELECT 'toDate';
SELECT to_date(to_datetime(1412106600), 'Europe/Moscow');
SELECT to_date(to_datetime(1412106600), 'Europe/Paris');
SELECT to_date(to_datetime(1412106600), 'Europe/London');
SELECT to_date(to_datetime(1412106600), 'Asia/Tokyo');
SELECT to_date(to_datetime(1412106600), 'Pacific/Pitcairn');

SELECT to_date(1412106600, 'Europe/Moscow');
SELECT to_date(1412106600, 'Europe/Paris');
SELECT to_date(1412106600, 'Europe/London');
SELECT to_date(1412106600, 'Asia/Tokyo');
SELECT to_date(1412106600, 'Pacific/Pitcairn');

SELECT to_date(16343);

/* to_string */

SELECT 'to_string';
SELECT to_string(to_datetime(1436956200), 'Europe/Moscow');
SELECT to_string(to_datetime(1436956200), 'Europe/Paris');
SELECT to_string(to_datetime(1436956200), 'Europe/London');
SELECT to_string(to_datetime(1436956200), 'Asia/Tokyo');
SELECT to_string(to_datetime(1436956200), 'Pacific/Pitcairn');

/* to_unix_timestamp */

SELECT 'to_unix_timestamp';
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Moscow'), 'Europe/Paris');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Moscow'), 'Europe/London');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Moscow'), 'Asia/Tokyo');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Moscow'), 'Pacific/Pitcairn');

SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Moscow'), 'Europe/Moscow');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/Paris'), 'Europe/Paris');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Europe/London'), 'Europe/London');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT to_unix_timestamp(to_string(to_datetime(1426415400), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* date_trunc */

SELECT 'date_trunc';

SELECT date_trunc('year', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('year', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('year', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('quarter', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('quarter', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('quarter', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('month', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('month', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('month', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('week', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('week', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('week', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('day', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('day', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('day', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('hour', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('hour', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('hour', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('minute', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('minute', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('minute', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('second', to_datetime('2020-01-01 04:11:22', 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('second', to_datetime('2020-01-01 12:11:22', 'Europe/London'), 'Europe/London');
SELECT date_trunc('second', to_datetime('2020-01-01 20:11:22', 'Europe/London'), 'Asia/Tokyo');

SELECT date_trunc('year', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('year', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('year', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('quarter', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('quarter', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('quarter', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('month', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('month', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('month', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('week', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('week', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('week', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('day', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('day', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('day', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('hour', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('hour', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('hour', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('minute', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('minute', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('minute', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');
SELECT date_trunc('second', toDateTime64('2020-01-01 04:11:22.123', 3, 'Europe/London'), 'America/Vancouver');
SELECT date_trunc('second', toDateTime64('2020-01-01 12:11:22.123', 3, 'Europe/London'), 'Europe/London');
SELECT date_trunc('second', toDateTime64('2020-01-01 20:11:22.123', 3, 'Europe/London'), 'Asia/Tokyo');

SELECT date_trunc('year', to_date('2020-01-01', 'Europe/London'));
SELECT date_trunc('quarter', to_date('2020-01-01', 'Europe/London'));
SELECT date_trunc('month', to_date('2020-01-01', 'Europe/London'));
SELECT date_trunc('week', to_date('2020-01-01', 'Europe/London'));
SELECT date_trunc('day', to_date('2020-01-01', 'Europe/London'), 'America/Vancouver');
