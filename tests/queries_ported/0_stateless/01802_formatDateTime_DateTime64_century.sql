-- { echo }

SELECT format_datetime(to_datetime64('1935-12-12 12:12:12', 0, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('1969-12-12 12:12:12', 0, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('1989-12-12 12:12:12', 0, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('2019-09-16 19:20:12', 0, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('2105-12-12 12:12:12', 0, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('2205-12-12 12:12:12', 0, 'Asia/Istanbul'), '%C');

-- non-zero scale
SELECT format_datetime(to_datetime64('1935-12-12 12:12:12', 6, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('1969-12-12 12:12:12', 6, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('1989-12-12 12:12:12', 6, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('2019-09-16 19:20:12', 0, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('2105-12-12 12:12:12', 6, 'Asia/Istanbul'), '%C');
SELECT format_datetime(to_datetime64('2205-01-12 12:12:12', 6, 'Asia/Istanbul'), '%C');