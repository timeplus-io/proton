SELECT 'parse_datetime64_best_effort_us';

SELECT
    s,
    parse_datetime64_best_effort_us(s,3,'UTC') AS a
FROM
(
    SELECT array_join([
'01-02-1930 12:00:00',
'12.02.1930 12:00:00',
'13/02/1930 12:00:00',
'02/25/1930 12:00:00'
]) AS s)
FORMAT PrettySpaceNoEscapes;

SELECT '';

SELECT 'parse_datetime64_best_effort_us_or_null';
SELECT parse_datetime64_best_effort_us_or_null('01/45/1925 16:00:00',3,'UTC');

SELECT 'parse_datetime64_best_effort_or_zero';
SELECT parse_datetime64_best_effort_or_zero('01/45/1925 16:00:00',3,'UTC');
