-- { echo }
-- tests with INT64_MIN (via overflow)
SELECT addMinutes(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
SELECT addHours(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
SELECT addWeeks(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
SELECT addDays(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
SELECT addYears(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
-- tests with INT64_MAX
SELECT addMinutes(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addHours(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addWeeks(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addDays(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT addYears(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
-- tests with inf
SELECT addMinutes(to_datetime('2021-01-01 00:00:00', 'GMT'), inf);
SELECT addHours(to_datetime('2021-01-01 00:00:00', 'GMT'), inf);
SELECT addWeeks(to_datetime('2021-01-01 00:00:00', 'GMT'), inf);
SELECT addDays(to_datetime('2021-01-01 00:00:00', 'GMT'), inf);
SELECT addYears(to_datetime('2021-01-01 00:00:00', 'GMT'), inf);
