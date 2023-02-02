SELECT
    to_date(to_datetime((number * 10) * 86400, 'Asia/Istanbul')) AS d1,
    to_date(to_datetime(number * 86400, 'Asia/Istanbul')) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL, 
    d1 WITH FILL STEP 5;

SELECT '===============';

SELECT
    to_date(to_datetime((number * 10) * 86400, 'Asia/Istanbul')) AS d1,
    to_date(to_datetime(number * 86400, 'Asia/Istanbul')) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
