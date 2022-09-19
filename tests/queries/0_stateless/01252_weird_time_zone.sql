SELECT 'Pacific/Kiritimati', to_datetime('2020-01-02 03:04:05', 'Pacific/Kiritimati') AS x, to_start_of_day(x), to_hour(x);
SELECT 'Africa/El_Aaiun', to_datetime('2020-01-02 03:04:05', 'Africa/El_Aaiun') AS x, to_start_of_day(x), to_hour(x);
SELECT 'Asia/Pyongyang', to_datetime('2020-01-02 03:04:05', 'Asia/Pyongyang') AS x, to_start_of_day(x), to_hour(x);
SELECT 'Pacific/Kwajalein', to_datetime('2020-01-02 03:04:05', 'Pacific/Kwajalein') AS x, to_start_of_day(x), to_hour(x);
SELECT 'Pacific/Apia', to_datetime('2020-01-02 03:04:05', 'Pacific/Apia') AS x, to_start_of_day(x), to_hour(x);
SELECT 'Pacific/Enderbury', to_datetime('2020-01-02 03:04:05', 'Pacific/Enderbury') AS x, to_start_of_day(x), to_hour(x);
SELECT 'Pacific/Fakaofo', to_datetime('2020-01-02 03:04:05', 'Pacific/Fakaofo') AS x, to_start_of_day(x), to_hour(x);

SELECT 'Pacific/Kiritimati', rand() as r, to_hour(to_datetime(r, 'Pacific/Kiritimati') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Africa/El_Aaiun', rand() as r, to_hour(to_datetime(r, 'Africa/El_Aaiun') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Asia/Pyongyang', rand() as r, to_hour(to_datetime(r, 'Asia/Pyongyang') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Kwajalein', rand() as r, to_hour(to_datetime(r, 'Pacific/Kwajalein') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Apia', rand() as r, to_hour(to_datetime(r, 'Pacific/Apia') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Enderbury', rand() as r, to_hour(to_datetime(r, 'Pacific/Enderbury') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Fakaofo', rand() as r, to_hour(to_datetime(r, 'Pacific/Fakaofo') AS t) AS h, t, to_type_name(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
