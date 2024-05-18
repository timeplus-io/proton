-- See comment in DateLUTImpl.cpp: "We doesn't support cases when time change results in switching to previous day..."
SELECT
    ignore(to_datetime(370641600, 'Asia/Istanbul') AS t),
    replace_regexp_all(to_string(t), '\\d', 'x'),
    to_hour(t) < 24,
    replace_regexp_all(format_datetime(t, '%Y-%m-%d %H:%i:%S; %R:%S; %F %T'), '\\d', 'x');
