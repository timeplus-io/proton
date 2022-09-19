DROP STREAM IF EXISTS A;

SELECT CAST(1 as DateTime64('abc')); -- { serverError 43 } # Invalid scale parameter type
SELECT CAST(1 as DateTime64(100)); -- { serverError 69 } # too big scale
SELECT CAST(1 as DateTime64(-1)); -- { serverError 43 } # signed scale parameter type
SELECT CAST(1 as DateTime64(3, 'qqq')); -- { serverError 1000 } # invalid timezone

SELECT toDateTime64('2019-09-16 19:20:11.234', 'abc'); -- { serverError 43 } # invalid scale
SELECT toDateTime64('2019-09-16 19:20:11.234', 100); -- { serverError 69 } # too big scale
SELECT toDateTime64(CAST([['CLb5Ph ']], 'string'), uniqHLL12('2Gs1V', 752)); -- { serverError 44 } # non-const string and non-const scale
SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'qqq'); -- { serverError 1000 } # invalid timezone

SELECT ignore(now64(gccMurmurHash())); -- { serverError 43 } # Illegal argument type
SELECT ignore(now64('abcd')); -- { serverError 43 } # Illegal argument type
SELECT ignore(now64(number)) FROM system.numbers LIMIT 10; -- { serverError 43 } # Illegal argument type
SELECT ignore(now64(3, 'invalid timezone')); -- { serverError 1000 }
SELECT ignore(now64(3, 1111)); -- { serverError 44 } # invalid timezone parameter type

WITH 'UTC' as timezone SELECT timezone, timeZoneOf(now64(3, timezone)) == timezone;
WITH 'Europe/Minsk' as timezone SELECT timezone, timeZoneOf(now64(3, timezone)) == timezone;

SELECT toDateTime64('2019-09-16 19:20:11', 3, 'UTC'); -- this now works OK and produces timestamp with no subsecond part

create stream A(t DateTime64(3, 'UTC')) ENGINE = MergeTree() ORDER BY t;
INSERT INTO A(t) VALUES ('2019-05-03 11:25:25.123456789');

SELECT to_string(t, 'UTC'), to_date(t), to_start_of_day(t), to_start_of_quarter(t), to_time(t), to_start_of_minute(t) FROM A ORDER BY t;

SELECT toDateTime64('2019-09-16 19:20:11.234', 3, 'Europe/Minsk');

DROP STREAM A;
