-- Error cases
SELECT parse_datetime64_best_effort();  -- {serverError 42}
SELECT parse_datetime64_best_effort(123);  -- {serverError 43}
SELECT parse_datetime64_best_effort('foo'); -- {serverError 41}

SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', 'bar');  -- {serverError 43} -- invalid scale parameter
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', 3, 4);  -- {serverError 43} -- invalid timezone parameter
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', 3, 'baz');  -- {serverError 1000} -- unknown timezone

SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', materialize(3), 4);  -- {serverError 44} -- non-const precision
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', 3, materialize('UTC'));  -- {serverError 44} -- non-const timezone

SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184012345678910111213141516171819Z', 3, 'UTC'); -- {serverError 6}

SELECT 'orNull';
SELECT parse_datetime64_best_effort_or_null('2020-05-14T03:37:03.253184Z', 3, 'UTC');
SELECT parse_datetime64_best_effort_or_null('foo', 3, 'UTC');

SELECT 'orZero';
SELECT parse_datetime64_best_effort_or_zero('2020-05-14T03:37:03.253184Z', 3, 'UTC');
SELECT parse_datetime64_best_effort_or_zero('bar', 3, 'UTC');

SELECT 'non-const';
SELECT parse_datetime64_best_effort(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC');

SELECT 'Timezones';
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', 3, 'UTC');
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk');

SELECT 'Formats';
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03.253184', 3, 'UTC');
SELECT parse_datetime64_best_effort('2020-05-14T03:37:03', 3, 'UTC');
SELECT parse_datetime64_best_effort('2020-05-14 03:37:03', 3, 'UTC');

SELECT 'Unix Timestamp with Milliseconds';
SELECT parse_datetime64_best_effort('1640649600123', 3, 'UTC');
SELECT parse_datetime64_best_effort('1640649600123', 1, 'UTC');
SELECT parse_datetime64_best_effort('1640649600123', 6, 'UTC');
