SELECT to_string(to_datetime('-922337203.6854775808', 1, 'Europe/Moscow'));
SELECT to_string(to_datetime('9922337203.6854775808', 1, 'Europe/Moscow'));
SELECT toDateTime64(CAST('10000000000.1' AS Decimal64(1)), 1, 'Europe/Moscow');
SELECT toDateTime64(CAST('-10000000000.1' AS Decimal64(1)), 1, 'Europe/Moscow');
