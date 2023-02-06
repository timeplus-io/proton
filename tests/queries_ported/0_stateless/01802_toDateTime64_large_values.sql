-- { echo }

SELECT to_datetime64('2205-12-12 12:12:12', 0, 'UTC');
SELECT to_datetime64('2205-12-12 12:12:12', 0, 'Asia/Istanbul');

SELECT to_datetime64('2205-12-12 12:12:12', 6, 'Asia/Istanbul');
SELECT to_datetime64('2205-12-12 12:12:12', 6, 'Asia/Istanbul');