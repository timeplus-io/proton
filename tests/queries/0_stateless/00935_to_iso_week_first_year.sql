SELECT to_date('1970-01-01') + number AS d, toISOWeek(d), toISOYear(d) FROM numbers(15);
-- Note that 1970-01-01 00:00:00 in Moscow is before unix epoch.
SELECT to_datetime(to_date('1970-01-02') + number, 'Europe/Moscow') AS t, toISOWeek(t), toISOYear(t) FROM numbers(15);
