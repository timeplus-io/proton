SELECT 'test intervals';

SELECT '- test nanoseconds';
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.123456789', 9), interval 1 nanosecond); -- in normal range, source scale matches result
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.1234567', 7), interval 1 nanosecond); -- in normal range, source scale less than result

select to_start_of_interval(a, interval 1 nanosecond) from ( select to_datetime64('1980-12-12 12:12:12.123456789', 9) as a ); -- non-constant argument

select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.123456789', 9), interval 1 nanosecond); -- below normal range, source scale matches result
select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.1234567', 7), interval 1 nanosecond); -- below normal range, source scale less than result

select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.123456789', 9), interval 1 nanosecond); -- above normal range, source scale matches result
select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.1234567', 7), interval 1 nanosecond); -- above normal range, source scale less than result


SELECT '- test microseconds';
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.123456', 6), interval 1 microsecond); -- in normal range, source scale matches result
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.1234', 4), interval 1 microsecond); -- in normal range, source scale less than result
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.12345678', 8), interval 1 microsecond); -- in normal range, source scale greater than result

select to_start_of_interval(a, interval 1 microsecond) from ( select to_datetime64('1980-12-12 12:12:12.12345678', 8) as a ); -- non-constant argument

select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.123456', 6), interval 1 microsecond); -- below normal range, source scale matches result
select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.1234', 4), interval 1 microsecond); -- below normal range, source scale less than result
select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.12345678', 8), interval 1 microsecond); -- below normal range, source scale greater than result


select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.123456', 6), interval 1 microsecond); -- above normal range, source scale matches result
select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.1234', 4), interval 1 microsecond); -- above normal range, source scale less than result
select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.12345678', 8), interval 1 microsecond); -- above normal range, source scale greater than result


SELECT '- test milliseconds';
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.123', 3), interval 1 millisecond); -- in normal range, source scale matches result
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.12', 2), interval 1 millisecond); -- in normal range, source scale less than result
select to_start_of_interval(to_datetime64('1980-12-12 12:12:12.123456', 6), interval 1 millisecond); -- in normal range, source scale greater than result

select to_start_of_interval(a, interval 1 millisecond) from ( select to_datetime64('1980-12-12 12:12:12.123456', 6) as a ); -- non-constant argument

select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.123', 3), interval 1 millisecond); -- below normal range, source scale matches result
select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.12', 2), interval 1 millisecond); -- below normal range, source scale less than result
select to_start_of_interval(to_datetime64('1930-12-12 12:12:12.123456', 6), interval 1 millisecond); -- below normal range, source scale greater than result

select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.123', 3), interval 1 millisecond); -- above normal range, source scale matches result
select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.12', 2), interval 1 millisecond); -- above normal range, source scale less than result
select to_start_of_interval(to_datetime64('2220-12-12 12:12:12.123456', 6), interval 1 millisecond); -- above normal range, source scale greater than result


SELECT 'test add[...]seconds()';


SELECT '- test nanoseconds';
select add_nanoseconds(to_datetime64('1980-12-12 12:12:12.123456789', 9), 1); -- In normal range, source scale matches result
select add_nanoseconds(to_datetime64('1980-12-12 12:12:12.1234567', 7), 1); -- In normal range, source scale less than result

select add_nanoseconds(a, 1) from ( select to_datetime64('1980-12-12 12:12:12.123456789', 9) AS a ); -- Non-constant argument

select add_nanoseconds(to_datetime64('1930-12-12 12:12:12.123456789', 9), 1); -- Below normal range, source scale matches result
select add_nanoseconds(to_datetime64('1930-12-12 12:12:12.1234567', 7), 1); -- Below normal range, source scale less than result

select add_nanoseconds(to_datetime64('2220-12-12 12:12:12.123456789', 9), 1); -- Above normal range, source scale matches result
select add_nanoseconds(to_datetime64('2220-12-12 12:12:12.1234567', 7), 1); -- Above normal range, source scale less than result


SELECT '- test microseconds';
select add_microseconds(to_datetime64('1980-12-12 12:12:12.123456', 6), 1); -- In normal range, source scale matches result
select add_microseconds(to_datetime64('1980-12-12 12:12:12.1234', 4), 1); -- In normal range, source scale less than result
select add_microseconds(to_datetime64('1980-12-12 12:12:12.12345678', 8), 1); -- In normal range, source scale greater than result

select add_microseconds(a, 1) from ( select to_datetime64('1980-12-12 12:12:12.123456', 6) AS a ); -- Non-constant argument

select add_microseconds(to_datetime64('1930-12-12 12:12:12.123456', 6), 1); -- Below normal range, source scale matches result
select add_microseconds(to_datetime64('1930-12-12 12:12:12.1234', 4), 1); -- Below normal range, source scale less than result
select add_microseconds(to_datetime64('1930-12-12 12:12:12.12345678', 8), 1); -- Below normal range, source scale greater than result

select add_microseconds(to_datetime64('2220-12-12 12:12:12.123456', 6), 1); -- Above normal range, source scale matches result
select add_microseconds(to_datetime64('2220-12-12 12:12:12.1234', 4), 1); -- Above normal range, source scale less than result
select add_microseconds(to_datetime64('2220-12-12 12:12:12.12345678', 8), 1); -- Above normal range, source scale greater than result


SELECT '- test milliseconds';
select add_milliseconds(to_datetime64('1980-12-12 12:12:12.123', 3), 1); -- In normal range, source scale matches result
select add_milliseconds(to_datetime64('1980-12-12 12:12:12.12', 2), 1); -- In normal range, source scale less than result
select add_milliseconds(to_datetime64('1980-12-12 12:12:12.123456', 6), 1); -- In normal range, source scale greater than result

select add_milliseconds(a, 1) from ( select to_datetime64('1980-12-12 12:12:12.123', 3) AS a ); -- Non-constant argument

select add_milliseconds(to_datetime64('1930-12-12 12:12:12.123', 3), 1); -- Below normal range, source scale matches result
select add_milliseconds(to_datetime64('1930-12-12 12:12:12.12', 2), 1); -- Below normal range, source scale less than result
select add_milliseconds(to_datetime64('1930-12-12 12:12:12.123456', 6), 1); -- Below normal range, source scale greater than result

select add_milliseconds(to_datetime64('2220-12-12 12:12:12.123', 3), 1); -- Above normal range, source scale matches result
select add_milliseconds(to_datetime64('2220-12-12 12:12:12.12', 2), 1); -- Above normal range, source scale less than result
select add_milliseconds(to_datetime64('2220-12-12 12:12:12.123456', 6), 1); -- Above normal range, source scale greater than result

select 'test subtract[...]seconds()';
select '- test nanoseconds';
select subtract_nanoseconds(to_datetime64('2023-01-01 00:00:00.0000000', 7, 'UTC'), 1);
select subtract_nanoseconds(to_datetime64('2023-01-01 00:00:00.0000000', 7, 'UTC'), 100);
select subtract_nanoseconds(to_datetime64('2023-01-01 00:00:00.0000000', 7, 'UTC'), -1);
select subtract_nanoseconds(to_datetime64('2023-01-01 00:00:00.0000000', 7, 'UTC'), -100);

select '- test microseconds';
select subtract_microseconds(to_datetime64('2023-01-01 00:00:00.0000', 4, 'UTC'), 1);
select subtract_microseconds(to_datetime64('2023-01-01 00:00:00.0000', 4, 'UTC'), 100);
select subtract_microseconds(to_datetime64('2023-01-01 00:00:00.0000', 4, 'UTC'), -1);
select subtract_microseconds(to_datetime64('2023-01-01 00:00:00.0000', 4, 'UTC'), -100);

select '- test milliseconds';
select subtract_milliseconds(to_datetime64('2023-01-01 00:00:00.0', 1, 'UTC'), 1);
select subtract_milliseconds(to_datetime64('2023-01-01 00:00:00.0', 1, 'UTC'), 100);
select subtract_milliseconds(to_datetime64('2023-01-01 00:00:00.0', 1, 'UTC'), -1);
select subtract_milliseconds(to_datetime64('2023-01-01 00:00:00.0', 1, 'UTC'), -100);
