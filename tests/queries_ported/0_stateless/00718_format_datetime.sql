 

SELECT format_datetime(); -- { serverError 42 }
SELECT format_datetime('not a datetime', 'IGNORED'); -- { serverError 43 }
SELECT format_datetime(now(), now()); -- { serverError 43 }
SELECT format_datetime(now(), 'good format pattern', now()); -- { serverError 43 }
SELECT format_datetime(now(), 'unescaped %'); -- { serverError 36 }
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%U'); -- { serverError 48 }
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%W'); -- { serverError 48 }

SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%C');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%d');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%D');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%e');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%F');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%H');
SELECT format_datetime(to_datetime('2018-01-02 02:33:44'), '%H');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%I');
SELECT format_datetime(to_datetime('2018-01-02 11:33:44'), '%I');
SELECT format_datetime(to_datetime('2018-01-02 00:33:44'), '%I');
SELECT format_datetime(to_datetime('2018-01-01 00:33:44'), '%j');
SELECT format_datetime(to_datetime('2000-12-31 00:33:44'), '%j');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%m');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%M');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%n');
SELECT format_datetime(to_datetime('2018-01-02 00:33:44'), '%p');
SELECT format_datetime(to_datetime('2018-01-02 11:33:44'), '%p');
SELECT format_datetime(to_datetime('2018-01-02 12:33:44'), '%p');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%R');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%S');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%t');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%T');
SELECT format_datetime(to_datetime('2018-01-01 22:33:44'), '%u'), format_datetime(to_datetime('2018-01-07 22:33:44'), '%u');
SELECT format_datetime(to_datetime('1996-01-01 22:33:44'), '%V'), format_datetime(to_datetime('1996-12-31 22:33:44'), '%V'),
       format_datetime(to_datetime('1999-01-01 22:33:44'), '%V'), format_datetime(to_datetime('1999-12-31 22:33:44'), '%V');
SELECT format_datetime(to_datetime('2018-01-01 22:33:44'), '%w'), format_datetime(to_datetime('2018-01-07 22:33:44'), '%w');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%y');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%Y');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), '%%');
SELECT format_datetime(to_datetime('2018-01-02 22:33:44'), 'no formatting pattern');

SELECT format_datetime(to_date('2018-01-01'), '%F %T');
SELECT
    format_datetime(to_datetime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'UTC'),
    format_datetime(to_datetime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'Europe/Moscow')