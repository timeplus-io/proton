
/* Test the DST(daylight saving time) offset changing boundary*/
SELECT 'DST boundary test for Europe/Moscow:';
SELECT number,(to_datetime('1981-04-01 22:40:00', 'Europe/Moscow') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, to_unix_timestamp(k) as s FROM numbers(4);
SELECT number,(to_datetime('1981-09-30 23:00:00', 'Europe/Moscow') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, to_unix_timestamp(k) as s FROM numbers(18);

SELECT 'DST boundary test for Asia/Tehran:';
SELECT number,(to_datetime('2020-03-21 22:40:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, to_unix_timestamp(k) as s FROM numbers(4);
SELECT number,(to_datetime('2020-09-20 23:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, to_unix_timestamp(k) as s FROM numbers(18);

SELECT 'DST boundary test for Australia/Lord_Howe. This is a special timezone with DST offset is 30mins with the timezone epoc also lays at half hour';
SELECT timezoneOffset(to_datetime('2018-08-21 22:20:00', 'Australia/Lord_Howe'));
SELECT timezoneOffset(to_datetime('2018-02-21 22:20:00', 'Australia/Lord_Howe'));

SELECT 'DST boundary test for Australia/Lord_Howe:';
SELECT number,(to_datetime('2020-10-04 01:40:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, to_unix_timestamp(k) as s FROM numbers(4);
SELECT number,(to_datetime('2019-04-07 01:00:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS k, timezoneOffset(k) AS t, to_unix_timestamp(k) as s FROM numbers(18);


/* The Batch Part. Test period is whole 4 days*/
SELECT '4 days test in batch comparing with manually computation result for Europe/Moscow:';
SELECT to_unix_timestamp(x) as tt, (to_datetime('1981-04-01 00:00:00', 'Europe/Moscow') + INTERVAL number * 600 SECOND) AS x, timezoneOffset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;
SELECT to_unix_timestamp(x) as tt, (to_datetime('1981-09-30 00:00:00', 'Europe/Moscow') + INTERVAL number * 600 SECOND) AS x, timezoneOffset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;

SELECT '4 days test in batch comparing with manually computation result for Asia/Tehran:';
SELECT to_unix_timestamp(x) as tt, (to_datetime('2020-03-21 00:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS x, timezoneOffset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;
SELECT to_unix_timestamp(x) as tt, (to_datetime('2020-09-20 00:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS x, timezoneOffset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;

SELECT '4 days test in batch comparing with manually computation result for Australia/Lord_Howe';
SELECT to_unix_timestamp(x) as tt, (to_datetime('2020-10-04 01:40:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS x, timezoneOffset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(18) where res != calc;
SELECT to_unix_timestamp(x) as tt, (to_datetime('2019-04-07 01:00:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS x, timezoneOffset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(18) where res != calc;


/* Find all the years had followed DST during given period*/

SELECT 'Moscow DST Years:';
SELECT number, (to_datetime('1970-06-01 00:00:00', 'Europe/Moscow') + INTERVAL number YEAR) AS DST_Y, timezoneOffset(DST_Y) AS t FROM numbers(51) where t != 10800;
SELECT 'Moscow DST Years with perment DST from 2011-2014:';
SELECT min((to_datetime('2011-01-01 00:00:00', 'Europe/Moscow') + INTERVAL number DAY) as day) as start, max(day) as end, count(1), concat(to_string(to_year(day)),'_',to_string(timezoneOffset(day)))as DST from numbers(365*4+1) group by DST order by start;

SELECT 'Tehran DST Years:';
SELECT number, (to_datetime('1970-06-01 00:00:00', 'Asia/Tehran') + INTERVAL number YEAR) AS DST_Y, timezoneOffset(DST_Y) AS t FROM numbers(51) where t != 12600;
SELECT 'Shanghai DST Years:';
SELECT number, (to_datetime('1970-08-01 00:00:00', 'Asia/Shanghai') + INTERVAL number YEAR) AS DST_Y, timezoneOffset(DST_Y) AS t FROM numbers(51) where t != 28800;

