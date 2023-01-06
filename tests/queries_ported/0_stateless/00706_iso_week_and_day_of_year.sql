-- International Programmers' Day
SELECT to_day_of_year(to_date('2018-09-13'));

SELECT to_date('2018-09-17') AS x, to_datetime(x) AS x_t, to_iso_week(x), to_iso_week(x_t), to_iso_year(x), to_iso_year(x_t), to_start_of_iso_year(x), to_start_of_iso_year(x_t);

SELECT to_date('2018-12-25') + number AS x, to_datetime(x) AS x_t, to_iso_week(x) AS w, to_iso_week(x_t) AS wt, to_iso_year(x) AS y, to_iso_year(x_t) AS yt, to_start_of_iso_year(x) AS ys, to_start_of_iso_year(x_t) AS yst, to_day_of_year(x) AS dy, to_day_of_year(x_t) AS dyt FROM system.numbers LIMIT 10;
SELECT to_date('2016-12-25') + number AS x, to_datetime(x) AS x_t, to_iso_week(x) AS w, to_iso_week(x_t) AS wt, to_iso_year(x) AS y, to_iso_year(x_t) AS yt, to_start_of_iso_year(x) AS ys, to_start_of_iso_year(x_t) AS yst, to_day_of_year(x) AS dy, to_day_of_year(x_t) AS dyt FROM system.numbers LIMIT 10;

-- ISO year always begins at monday.
SELECT DISTINCT to_day_of_week(to_start_of_iso_year(to_datetime(1000000000 + rand64() % 1000000000))) FROM numbers(10000);
SELECT DISTINCT to_day_of_week(to_start_of_iso_year(to_date(10000 + rand64() % 20000))) FROM numbers(10000);

-- Year and ISO year don't differ by more than one.
WITH to_datetime(1000000000 + rand64() % 1000000000) AS time SELECT max(abs(to_year(time) - to_iso_year(time))) <= 1 FROM numbers(10000);

-- ISO week is between 1 and 53
WITH to_datetime(1000000000 + rand64() % 1000000000) AS time SELECT DISTINCT to_iso_week(time) BETWEEN 1 AND 53 FROM numbers(1000000);
