SET send_logs_level = 'fatal';

SELECT formatDateTime(); -- { serverError 42 }
SELECT formatDateTime('not a datetime', 'IGNORED'); -- { serverError 43 }
SELECT formatDateTime(now(), now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'good format pattern', now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'unescaped %'); -- { serverError 36 }
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%U'); -- { serverError 48 }
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%W'); -- { serverError 48 }

SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%C');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%d');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%D');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%e');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%F');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%H');
SELECT formatDateTime(to_datetime('2018-01-02 02:33:44'), '%H');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%I');
SELECT formatDateTime(to_datetime('2018-01-02 11:33:44'), '%I');
SELECT formatDateTime(to_datetime('2018-01-02 00:33:44'), '%I');
SELECT formatDateTime(to_datetime('2018-01-01 00:33:44'), '%j');
SELECT formatDateTime(to_datetime('2000-12-31 00:33:44'), '%j');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%m');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%M');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%n');
SELECT formatDateTime(to_datetime('2018-01-02 00:33:44'), '%p');
SELECT formatDateTime(to_datetime('2018-01-02 11:33:44'), '%p');
SELECT formatDateTime(to_datetime('2018-01-02 12:33:44'), '%p');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%R');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%S');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%t');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%T');
SELECT formatDateTime(to_datetime('2018-01-01 22:33:44'), '%u'), formatDateTime(to_datetime('2018-01-07 22:33:44'), '%u');
SELECT formatDateTime(to_datetime('1996-01-01 22:33:44'), '%V'), formatDateTime(to_datetime('1996-12-31 22:33:44'), '%V'),
       formatDateTime(to_datetime('1999-01-01 22:33:44'), '%V'), formatDateTime(to_datetime('1999-12-31 22:33:44'), '%V');
SELECT formatDateTime(to_datetime('2018-01-01 22:33:44'), '%w'), formatDateTime(to_datetime('2018-01-07 22:33:44'), '%w');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%y');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%Y');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), '%%');
SELECT formatDateTime(to_datetime('2018-01-02 22:33:44'), 'no formatting pattern');

SELECT formatDateTime(to_date('2018-01-01'), '%F %T');
SELECT
    formatDateTime(to_datetime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'UTC'),
    formatDateTime(to_datetime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'Europe/Moscow')