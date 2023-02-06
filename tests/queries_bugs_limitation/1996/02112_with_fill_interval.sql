DROP STREAM IF EXISTS with_fill_date;
CREATE STREAM with_fill_date (d Date, d32 Date32) ENGINE = Memory;

INSERT INTO with_fill_date VALUES (to_date('2020-02-05'), to_date('2020-02-05'));
INSERT INTO with_fill_date VALUES (to_date('2020-02-16'), to_date('2020-02-16'));
INSERT INTO with_fill_date VALUES (to_date('2020-03-03'), to_date('2020-03-03'));
INSERT INTO with_fill_date VALUES (to_date('2020-06-10'), to_date('2020-06-10'));

SELECT '1 DAY';
SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 DAY LIMIT 5;
SELECT '1 WEEK';
SELECT to_start_of_week(d) as d, count() from with_fill_date group by d order by d with fill step interval 1 week limit 5;
SELECT '1 MONTH';
SELECT to_start_of_month(d) as d, count() from with_fill_date group by d order by d with fill step interval 1 month limit 5;
SELECT '3 MONTH';
SELECT to_start_of_month(d) as d, count() from with_fill_date group by d order by d with fill
    FROM to_date('2020-01-01')
    TO to_date('2021-01-01')
    STEP INTERVAL 3 MONTH;

SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 HOUR LIMIT 5; -- { serverError 475 }

SELECT '1 DAY';
SELECT d32, count() FROM with_fill_date GROUP BY d32 ORDER BY d32 WITH FILL STEP INTERVAL 1 DAY LIMIT 5;
SELECT '1 WEEK';
SELECT to_start_of_week(d32) as d32, count() from with_fill_date group by d32 order by d32 with fill step interval 1 week limit 5;
SELECT '1 MONTH';
SELECT to_start_of_month(d32) as d32, count() from with_fill_date group by d32 order by d32 with fill step interval 1 month limit 5;
SELECT '3 MONTH';
SELECT to_start_of_month(d32) as d32, count() from with_fill_date group by d32 order by d32 with fill
    FROM to_date('2020-01-01')
    TO to_date('2021-01-01')
    STEP INTERVAL 3 MONTH;

SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 HOUR LIMIT 5; -- { serverError 475 }

DROP STREAM with_fill_date;

DROP STREAM IF EXISTS with_fill_date;
CREATE STREAM with_fill_date (d DateTime('UTC'), d64 DateTime64(3, 'UTC')) ENGINE = Memory;

INSERT INTO with_fill_date VALUES (to_datetime('2020-02-05 10:20:00', 'UTC'), to_datetime64('2020-02-05 10:20:00', 3, 'UTC'));
INSERT INTO with_fill_date VALUES (to_datetime('2020-03-08 11:01:00', 'UTC'), to_datetime64('2020-03-08 11:01:00', 3, 'UTC'));

SELECT '15 MINUTE';
SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 15 MINUTE LIMIT 5;
SELECT '6 HOUR';
SELECT to_start_of_hour(d) as d, count() from with_fill_date group by d order by d with fill step interval 6 hour limit 5;
SELECT '10 DAY';
SELECT to_start_of_day(d) as d, count() from with_fill_date group by d order by d with fill step interval 10 day limit 5;

SELECT '15 MINUTE';
SELECT d64, count() FROM with_fill_date GROUP BY d64 ORDER BY d64 WITH FILL STEP INTERVAL 15 MINUTE LIMIT 5;
SELECT '6 HOUR';
SELECT to_start_of_hour(d64) as d64, count() from with_fill_date group by d64 order by d64 with fill step interval 6 hour limit 5;
SELECT '10 DAY';
SELECT to_start_of_day(d64) as d64, count() from with_fill_date group by d64 order by d64 with fill step interval 10 day limit 5;

DROP STREAM with_fill_date;

SELECT number FROM numbers(100) ORDER BY number WITH FILL STEP INTERVAL 1 HOUR; -- { serverError 475 }

CREATE STREAM with_fill_date (d Date, id uint32) ENGINE = Memory;

INSERT INTO with_fill_date VALUES (to_date('2020-02-05'), 1);
INSERT INTO with_fill_date VALUES (to_date('2020-02-16'), 3);
INSERT INTO with_fill_date VALUES (to_date('2020-03-10'), 2);
INSERT INTO with_fill_date VALUES (to_date('2020-03-03'), 3);

SELECT '1 MONTH';

SELECT to_start_of_month(d) as d, id, count() from with_fill_date
GROUP BY d, id
ORDER BY
d WITH FILL
    FROM to_date('2020-01-01')
    TO to_date('2020-05-01')
    STEP INTERVAL 1 MONTH,
id WITH FILL FROM 1 TO 5;

DROP STREAM with_fill_date;

SELECT d FROM (SELECT to_date(1) AS d)
ORDER BY d DESC WITH FILL FROM to_date(3) TO to_date(0) STEP INTERVAL -1 DAY;
