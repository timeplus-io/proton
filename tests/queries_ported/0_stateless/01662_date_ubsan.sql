-- tests with INT64_MAX
SELECT add_minutes(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT add_hours(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT add_weeks(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT add_days(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
SELECT add_years(to_datetime('2020-01-01 00:00:00', 'GMT'), 9223372036854775807);
-- tests with INT64_MIN (via overflow)
SELECT add_minutes(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError DECIMAL_OVERFLOW }
SELECT add_hours(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError DECIMAL_OVERFLOW }
SELECT add_weeks(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError DECIMAL_OVERFLOW }
SELECT add_days(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError DECIMAL_OVERFLOW }
SELECT add_years(to_datetime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808); -- { serverError DECIMAL_OVERFLOW }
-- tests with inf
SELECT add_minutes(to_datetime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError DECIMAL_OVERFLOW }
SELECT add_hours(to_datetime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError DECIMAL_OVERFLOW }
SELECT add_weeks(to_datetime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError DECIMAL_OVERFLOW }
SELECT add_days(to_datetime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError DECIMAL_OVERFLOW }
SELECT add_years(to_datetime('2021-01-01 00:00:00', 'GMT'), inf); -- { serverError DECIMAL_OVERFLOW }
