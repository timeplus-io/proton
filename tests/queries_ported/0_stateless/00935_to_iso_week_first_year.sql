SELECT to_date('1970-01-01') + number AS d, to_iso_week(d), to_iso_year(d) FROM numbers(15);
-- Note that 1970-01-01 00:00:00 in Moscow is before unix epoch.
SELECT to_datetime(to_date('1970-01-02') + number, 'Europe/Moscow') AS t, to_iso_week(t), to_iso_year(t) FROM numbers(15);
