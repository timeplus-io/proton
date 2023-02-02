SELECT 'parse_datetime_best_effort_us';

SELECT
    s,
    parse_datetime_best_effort_us(s, 'UTC') AS a
FROM
(
    SELECT array_join([
'1970/01/02 010203Z',
'01-02-2001 UTC',
'10.23.1990',
'01-02-2017 03:04:05+1',
'01/02/2017 03:04:05+300',
'01.02.2017 03:04:05GMT',
'01-02-2017 03:04:05 MSD',
'01-02-2017 11:04:05 AM',
'01-02-2017 11:04:05 PM',
'01-02-2017 12:04:05 AM',
'01-02-2017 12:04:05 PM',
'01.02.17 03:04:05 MSD Feb',
'01/02/2017 03:04:05 MSK',
'12/13/2019',
'13/12/2019',
'03/04/2019'
]) AS s)
FORMAT PrettySpaceNoEscapes;

SELECT 'parse_datetime_best_effort_us_or_zero', 'parse_datetime_best_effort_us_or_null';
SELECT
    s,
    parse_datetime_best_effort_us_or_zero(s, 'UTC') AS a,
    parse_datetime_best_effort_us_or_null(s, 'UTC') AS b
FROM
(
    SELECT array_join([
'1970/01/02 010203Z',
'01-02-2001 UTC',
'10.23.1990',
'01-02-2017 03:04:05+1',
'01/02/2017 03:04:05+300',
'01.02.2017 03:04:05GMT',
'01-02-2017 03:04:05 MSD',
'01-02-2017 11:04:05 AM',
'01-02-2017 11:04:05 PM',
'01-02-2017 12:04:05 AM',
'01-02-2017 12:04:05 PM',
'01.02.17 03:04:05 MSD Feb',
'01/02/2017 03:04:05 MSK',
'12/13/2019',
'13/12/2019',
'03/04/2019',
'',
'xyz'
]) AS s)
FORMAT PrettySpaceNoEscapes;
