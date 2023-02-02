/* to_datetime or to_string or other functions which should call the toMinute() function will all meet this bug. tests below will verify the to_datetime and to_string. */
SELECT 'Check the bug causing situation: the special Australia/Lord_Howe time zone. to_datetime and to_string functions are all tested at once';
SELECT to_unix_timestamp(x) as tt, (to_datetime('2019-04-07 01:00:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS x,  to_string(x) as xx FROM numbers(20);

/* The Batch Part. Test period is whole 4 days*/
SELECT '4 days test in batch comparing with manually computation result for Asia/Istanbul whose timezone epoc is of whole hour:';
SELECT to_unix_timestamp(x) as tt, (to_datetime('1981-04-01 00:00:00', 'Asia/Istanbul') + INTERVAL number * 600 SECOND) AS x, timezone_offset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;
SELECT to_unix_timestamp(x) as tt, (to_datetime('1981-09-30 00:00:00', 'Asia/Istanbul') + INTERVAL number * 600 SECOND) AS x, timezone_offset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;

SELECT '4 days test in batch comparing with manually computation result for Asia/Tehran whose timezone epoc is of half hour:';
SELECT to_unix_timestamp(x) as tt, (to_datetime('2020-03-21 00:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS x, timezone_offset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;
SELECT to_unix_timestamp(x) as tt, (to_datetime('2020-09-20 00:00:00', 'Asia/Tehran') + INTERVAL number * 600 SECOND) AS x, timezone_offset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;

SELECT '4 days test in batch comparing with manually computation result for Australia/Lord_Howe whose timezone epoc is of half hour and also its DST offset is half hour:';
SELECT to_unix_timestamp(x) as tt, (to_datetime('2020-10-04 01:40:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS x, timezone_offset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;
SELECT to_unix_timestamp(x) as tt, (to_datetime('2019-04-07 01:00:00', 'Australia/Lord_Howe') + INTERVAL number * 600 SECOND) AS x, timezone_offset(x) as res,(to_datetime(to_string(x), 'UTC') - x ) AS calc FROM numbers(576) where res != calc;
