-- See comment in DateLUTImpl.cpp: "We doesn't support cases when time change results in switching to previous day..."
SELECT
    ignore(to_datetime(370641600, 'Europe/Moscow') AS t),
    replaceRegexpAll(to_string(t), '\\d', 'x'),
    to_hour(t) < 24,
    replaceRegexpAll(formatDateTime(t, '%Y-%m-%d %H:%M:%S; %R:%S; %F %T'), '\\d', 'x');
