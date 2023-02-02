DROP STREAM IF EXISTS test;

CREATE STREAM test (a DateTime, b DateTime(), c DateTime(2), d DateTime('Asia/Istanbul'), e DateTime(3, 'Asia/Istanbul'), f DateTime32, g DateTime32('Asia/Istanbul'), h DateTime(0)) ENGINE = MergeTree ORDER BY a;

INSERT INTO test VALUES('2020-01-01 00:00:00', '2020-01-01 00:01:00', '2020-01-01 00:02:00.11', '2020-01-01 00:03:00', '2020-01-01 00:04:00.22', '2020-01-01 00:05:00', '2020-01-01 00:06:00', '2020-01-01 00:06:00');

SELECT a, to_type_name(a), b, to_type_name(b), c, to_type_name(c), d, to_type_name(d), e, to_type_name(e), f, to_type_name(f), g, to_type_name(g), h, to_type_name(h) FROM test;

SELECT to_datetime('2020-01-01 00:00:00') AS a, to_type_name(a), to_datetime('2020-01-01 00:02:00.11', 2) AS b, to_type_name(b), to_datetime('2020-01-01 00:03:00', 'Asia/Istanbul') AS c, to_type_name(c), to_datetime('2020-01-01 00:04:00.22', 3, 'Asia/Istanbul') AS d, to_type_name(d), to_datetime('2020-01-01 00:05:00', 0) AS e, to_type_name(e);

SELECT CAST('2020-01-01 00:00:00', 'DateTime') AS a, to_type_name(a), CAST('2020-01-01 00:02:00.11', 'DateTime(2)') AS b, to_type_name(b), CAST('2020-01-01 00:03:00', 'DateTime(\'Asia/Istanbul\')') AS c, to_type_name(c), CAST('2020-01-01 00:04:00.22', 'DateTime(3, \'Asia/Istanbul\')') AS d, to_type_name(d), CAST('2020-01-01 00:05:00', 'DateTime(0)') AS e, to_type_name(e);

SELECT to_datetime32('2020-01-01 00:00:00') AS a, to_type_name(a);

SELECT 'parse_datetime_best_effort';
SELECT parse_datetime_best_effort('<Empty>', 3) AS a, to_type_name(a); -- {serverError 41}
SELECT parse_datetime_best_effort('2020-05-14T03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14 03:37:03', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14 11:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14 11:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14 12:37:03 AM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14 12:37:03 PM', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14T03:37:03.253184', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14T03:37:03.253184Z', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('2020-05-14T03:37:03.253184Z', 3, 'Europe/Minsk') AS a, to_type_name(a);
SELECT parse_datetime_best_effort(materialize('2020-05-14T03:37:03.253184Z'), 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('1640649600123', 3, 'UTC') AS a, to_type_name(a);
SELECT parse_datetime_best_effort('1640649600123', 'UTC') AS a, to_type_name(a);

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

SELECT 'parse_datetime32_best_effort';
SELECT parse_datetime32_best_effort('<Empty>') AS a, to_type_name(a); -- {serverError 41}
SELECT parse_datetime32_best_effort('2020-05-14T03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14 03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14 11:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14 11:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14 12:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14 12:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14T03:37:03.253184', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14T03:37:03.253184Z', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parse_datetime32_best_effortOrNull';
SELECT parse_datetime32_best_effort_or_null('<Empty>') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14T03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14 03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14 11:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14 11:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14 12:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14 12:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14T03:37:03.253184', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14T03:37:03.253184Z', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_null('1640649600123', 'UTC') AS a, to_type_name(a);

SELECT 'parse_datetime32_best_effort_or_zero';
SELECT parse_datetime32_best_effort_or_zero('<Empty>', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14T03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14 03:37:03', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14 11:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14 11:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14 12:37:03 AM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14 12:37:03 PM', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14T03:37:03.253184', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14T03:37:03.253184Z', 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('2020-05-14T03:37:03.253184Z', 'Europe/Minsk') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero(materialize('2020-05-14T03:37:03.253184Z'), 'UTC') AS a, to_type_name(a);
SELECT parse_datetime32_best_effort_or_zero('1640649600123', 'UTC') AS a, to_type_name(a);

DROP STREAM IF EXISTS test;
