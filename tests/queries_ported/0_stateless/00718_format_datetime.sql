-- SET send_logs_level = 'fatal';

-- SELECT format_datetime(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH (42) }
-- SELECT format_datetime('not a datetime', 'IGNORED'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT (43) }
-- SELECT format_datetime(now(), now()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT (43) }
-- SELECT format_datetime(now(), 'good format pattern', now()); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT (43) }
-- SELECT format_datetime(now(), 'unescaped %'); -- { serverError BAD_ARGUMENTS (36) }
-- SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%U'); -- { serverError NOT_IMPLEMENTED (48) }
-- SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%v'); -- { serverError NOT_IMPLEMENTED (48) }
-- SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%x'); -- { serverError NOT_IMPLEMENTED (48) }
-- SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%X'); -- { serverError NOT_IMPLEMENTED (48) }

SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%a'), format_datetime(to_date('2018-01-02'), '%a');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%b'), format_datetime(to_date('2018-01-02'), '%b');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%c'), format_datetime(to_date('2018-01-02'), '%c');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%C'), format_datetime(to_date('2018-01-02'), '%C');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%d'), format_datetime(to_date('2018-01-02'), '%d');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%D'), format_datetime(to_date('2018-01-02'), '%D');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%e'), format_datetime(to_date('2018-01-02'), '%e');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%F'), format_datetime(to_date('2018-01-02'), '%F');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%h'), format_datetime(to_date('2018-01-02'), '%h');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%H'), format_datetime(to_date('2018-01-02'), '%H');
SELECT format_datetime(to_datetime('2018-01-02 02:33:44'), '%H');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%i'), format_datetime(to_date('2018-01-02'), '%i');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%I'), format_datetime(to_date('2018-01-02'), '%I');
SELECT format_datetime(to_datetime('2018-01-02 11:33:44'), '%I');
SELECT format_datetime(to_datetime('2018-01-02 00:33:44'), '%I');
SELECT format_datetime(to_datetime('2018-01-01 00:33:44'), '%j'), format_datetime(to_date('2018-01-01'), '%j');
SELECT format_datetime(to_datetime('2000-12-31 00:33:44'), '%j'), format_datetime(to_date('2000-12-31'), '%j');
SELECT format_datetime(to_datetime('2000-12-31 00:33:44'), '%k'), format_datetime(to_date('2000-12-31'), '%k');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%m'), format_datetime(to_date('2018-01-02'), '%m');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%M'), format_datetime(to_date('2018-01-02'), '%M') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 1;
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%M'), format_datetime(to_date('2018-01-02'), '%M') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%n'), format_datetime(to_date('2018-01-02'), '%n');
SELECT format_datetime(to_datetime('2018-01-02 00:33:44'), '%p'), format_datetime(to_datetime('2018-01-02'), '%p');
SELECT format_datetime(to_datetime('2018-01-02 11:33:44'), '%p');
SELECT format_datetime(to_datetime('2018-01-02 12:33:44'), '%p');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%r'), format_datetime(to_date('2018-01-02'), '%r');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%R'), format_datetime(to_date('2018-01-02'), '%R');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%S'), format_datetime(to_date('2018-01-02'), '%S');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%t'), format_datetime(to_date('2018-01-02'), '%t');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%T'), format_datetime(to_date('2018-01-02'), '%T');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%W'), format_datetime(to_date('2018-01-02'), '%W');
SELECT format_datetime(to_datetime('2018-01-01 22:33:44'), '%u'), format_datetime(to_datetime('2018-01-07 22:33:44'), '%u'),
       format_datetime(to_date('2018-01-01'), '%u'), format_datetime(to_date('2018-01-07'), '%u');
SELECT format_datetime(to_datetime('1996-01-01 22:33:44'), '%V'), format_datetime(to_datetime('1996-12-31 22:33:44'), '%V'),
       format_datetime(to_datetime('1999-01-01 22:33:44'), '%V'), format_datetime(to_datetime('1999-12-31 22:33:44'), '%V'),
       format_datetime(to_date('1996-01-01'), '%V'), format_datetime(to_date('1996-12-31'), '%V'),
       format_datetime(to_date('1999-01-01'), '%V'), format_datetime(to_date('1999-12-31'), '%V');
SELECT format_datetime(to_datetime('2018-01-01 22:33:44'), '%w'), format_datetime(to_datetime('2018-01-07 22:33:44'), '%w'),
       format_datetime(to_date('2018-01-01'), '%w'), format_datetime(to_date('2018-01-07'), '%w');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%y'), format_datetime(to_date('2018-01-02'), '%y');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%Y'), format_datetime(to_date('2018-01-02'), '%Y');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%%'), format_datetime(to_date('2018-01-02'), '%%');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), 'no formatting pattern'), format_datetime(to_date('2018-01-02'), 'no formatting pattern');

SELECT format_datetime(to_date('2018-01-01'), '%F %T');
SELECT format_datetime(to_date('1927-01-01'), '%F %T');

SELECT
    format_datetime(to_datetime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'UTC'),
    format_datetime(to_datetime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'Asia/Istanbul');

SELECT format_datetime(to_datetime('2020-01-01 01:00:00', 'UTC'), '%z');
SELECT format_datetime(to_datetime('2020-01-01 01:00:00', 'US/Samoa'), '%z');
SELECT format_datetime(to_datetime('2020-01-01 01:00:00', 'Europe/Moscow'), '%z');
SELECT format_datetime(to_datetime('1970-01-01 00:00:00', 'Asia/Kolkata'), '%z');

select format_datetime(to_datetime64('2010-01-04 12:34:56.123456', 7), '%f');
select format_datetime(to_datetime64('2022-12-08 18:11:29.00034', 6, 'UTC'), '%f');

select format_datetime(to_datetime64('2022-12-08 18:11:29.1234', 9, 'UTC'), '%F %T.%f');
select format_datetime(to_datetime64('2022-12-08 18:11:29.1234', 1, 'UTC'), '%F %T.%f');
select format_datetime(to_datetime64('2022-12-08 18:11:29.1234', 0, 'UTC'), '%F %T.%f');
select format_datetime(to_datetime('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
select format_datetime(to_date('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
select format_datetime(to_date('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
