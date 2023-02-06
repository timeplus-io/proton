drop stream if exists test_tz_hour;

create stream test_tz_hour(t DateTime, x string) engine MergeTree partition by to_YYYYMMDD(t) order by x;
insert into test_tz_hour select to_datetime('2021-06-01 00:00:00', 'UTC') + number * 600, 'x' from numbers(1e3);

select to_hour(to_timezone(t, 'UTC')) as to_hour_UTC, to_hour(to_timezone(t, 'Asia/Jerusalem')) as to_hour_Israel, count() from test_tz_hour where to_hour_Israel = 8 group by to_hour_UTC, to_hour_Israel;

drop stream test_tz_hour;
