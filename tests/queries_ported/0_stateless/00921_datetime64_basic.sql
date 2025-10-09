DROP STREAM IF EXISTS A;

SELECT CAST(1 as datetime64('abc')); -- { serverError 43 } # Invalid scale parameter type
SELECT CAST(1 as datetime64(100)); -- { serverError 69 } # too big scale
SELECT CAST(1 as datetime64(-1)); -- { serverError 43 } # signed scale parameter type
SELECT CAST(1 as datetime64(3, 'qqq')); -- { serverError BAD_ARGUMENTS } # invalid timezone

SELECT to_datetime64('2019-09-16 19:20:11.234', 'abc'); -- { serverError 43 } # invalid scale
SELECT to_datetime64('2019-09-16 19:20:11.234', 100); -- { serverError 69 } # too big scale
SELECT to_datetime64(CAST([['CLb5Ph ']], 'string'), uniq_hll12('2Gs1V', 752)); -- { serverError 44 } # non-const string and non-const scale
SELECT to_datetime64('2019-09-16 19:20:11.234', 3, 'qqq'); -- { serverError BAD_ARGUMENTS } # invalid timezone

SELECT ignore(now64(gcc_murmur_hash())); -- { serverError 43 } # Illegal argument type
SELECT ignore(now64('abcd')); -- { serverError 43 } # Illegal argument type
SELECT ignore(now64(number)) FROM system.numbers LIMIT 10; -- { serverError 43 } # Illegal argument type
SELECT ignore(now64(3, 'invalid timezone')); -- { serverError BAD_ARGUMENTS }
SELECT ignore(now64(3, 1111)); -- { serverError 44 } # invalid timezone parameter type

WITH 'UTC' as timezone SELECT timezone, timezone_of(now64(3, timezone)) == timezone;
WITH 'Europe/Minsk' as timezone SELECT timezone, timezone_of(now64(3, timezone)) == timezone;

SELECT to_datetime64('2019-09-16 19:20:11', 3, 'UTC'); -- this now works OK and produces timestamp with no subsecond part

create stream A(t datetime64(3, 'UTC'));
select sleep(1) FORMAT Null;
INSERT INTO A(t) VALUES ('2019-05-03 11:25:25.123456789');
select sleep(3) FORMAT Null;

select to_time(t) FROM table(A); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT} # not supported to_time(type!=string) why?
SELECT to_string(t, 'UTC'), to_date(t), to_start_of_day(t), to_start_of_quarter(t), to_time(to_string(t)), to_start_of_minute(t) FROM table(A) ORDER BY t;

SELECT to_datetime64('2019-09-16 19:20:11.234', 3, 'Europe/Minsk');

DROP STREAM A;
