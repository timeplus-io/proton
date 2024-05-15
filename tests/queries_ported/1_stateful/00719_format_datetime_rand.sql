-- We add 1, because function to_string has special behaviour for zero datetime
WITH to_datetime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE format_datetime(t, '%F %T') != to_string(t);
WITH to_datetime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE format_datetime(t, '%Y-%m-%d %H:%i:%S') != to_string(t);
WITH to_datetime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE format_datetime(t, '%Y-%m-%d %R:%S') != to_string(t);
WITH to_datetime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE format_datetime(t, '%F %R:%S') != to_string(t);

WITH to_date(today() + rand() % 4096) AS t SELECT count() FROM numbers(1000000) WHERE format_datetime(t, '%F') != to_string(t);

-- Note: in some other timezones, daylight saving time change happens in midnight, so the first time of day is 01:00:00 instead of 00:00:00.
-- Stick to Moscow timezone to avoid this issue.
WITH to_date(today() + rand() % 4096) AS t SELECT count() FROM numbers(1000000) WHERE format_datetime(t, '%F %T', 'Europe/Moscow') != to_string(to_datetime(t, 'Europe/Moscow'));
