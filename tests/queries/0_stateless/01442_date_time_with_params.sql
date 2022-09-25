DROP STREAM IF EXISTS test;

create stream test (a datetime, b datetime(), c datetime(2), d datetime('Europe/Moscow'), e datetime(3, 'Europe/Moscow'), f DateTime32, g DateTime32('Europe/Moscow'), h datetime(0)) ENGINE = MergeTree ORDER BY a;

INSERT INTO test VALUES('2020-01-01 00:00:00', '2020-01-01 00:01:00', '2020-01-01 00:02:00.11', '2020-01-01 00:03:00', '2020-01-01 00:04:00.22', '2020-01-01 00:05:00', '2020-01-01 00:06:00', '2020-01-01 00:06:00');

SELECT a, to_type_name(a), b, to_type_name(b), c, to_type_name(c), d, to_type_name(d), e, to_type_name(e), f, to_type_name(f), g, to_type_name(g), h, to_type_name(h) FROM test;

SELECT to_datetime('2020-01-01 00:00:00') AS a, to_type_name(a), to_datetime('2020-01-01 00:02:00.11', 2) AS b, to_type_name(b), to_datetime('2020-01-01 00:03:00', 'Europe/Moscow') AS c, to_type_name(c), to_datetime('2020-01-01 00:04:00.22', 3, 'Europe/Moscow') AS d, to_type_name(d), to_datetime('2020-01-01 00:05:00', 0) AS e, to_type_name(e);

SELECT CAST('2020-01-01 00:00:00', 'datetime') AS a, to_type_name(a), CAST('2020-01-01 00:02:00.11', 'datetime(2)') AS b, to_type_name(b), CAST('2020-01-01 00:03:00', 'datetime(\'Europe/Moscow\')') AS c, to_type_name(c), CAST('2020-01-01 00:04:00.22', 'datetime(3, \'Europe/Moscow\')') AS d, to_type_name(d), CAST('2020-01-01 00:05:00', 'datetime(0)') AS e, to_type_name(e);

SELECT toDateTime32('2020-01-01 00:00:00') AS a, to_type_name(a);

SELECT 'parseDateTimeBestEffort';
SELECT parseDateTimeBestEffort('<Empty>', 3) AS a, to_type_name(a); -- {serverError 41}
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14 03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('1640649600123', 3, 'UTC') AS a, to_type_name(a);
SELECT parseDateTimeBestEffort('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parse_datetime_best_effort_or_null';
SELECT parse_datetime_best_effort_or_null('<Empty>', 3) AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14T03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14 03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('1640649600123', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_null('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parse_datetime_best_effort_or_zero';
SELECT parse_datetime_best_effort_or_zero('<Empty>', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14T03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14 03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('1640649600123', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort_or_zero('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parseDateTime32BestEffort';
SELECT parseDateTime32BestEffort('<Empty>') AS a, to_type_name(a); -- {serverError 41}
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14 03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14 11:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14 11:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14 12:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14 12:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03.253184', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03.253184Z', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffort('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parseDateTime32BestEffortOrNull';
SELECT parseDateTime32BestEffortOrNull('<Empty>') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 11:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 11:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 12:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14 12:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03.253184', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03.253184Z', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrNull('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parseDateTime32BestEffortOrZero';
SELECT parseDateTime32BestEffortOrZero('<Empty>', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 11:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 11:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 12:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14 12:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03.253184', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03.253184Z', 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, to_type_name(a);
SELECT parseDateTime32BestEffortOrZero('1640649600123', 'UTC') AS a, to_type_name(a);

DROP STREAM IF EXISTS test;
