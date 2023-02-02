SELECT to_string(to_datetime('-922337203.6854775808', 1, 'Asia/Istanbul'));
SELECT to_string(to_datetime('9922337203.6854775808', 1, 'Asia/Istanbul'));
SELECT to_datetime64(CAST('10500000000.1' AS decimal64(1)), 1, 'Asia/Istanbul');
SELECT to_datetime64(CAST('-10500000000.1' AS decimal64(1)), 1, 'Asia/Istanbul');
