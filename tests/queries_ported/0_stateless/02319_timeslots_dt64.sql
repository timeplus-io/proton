SELECT time_slots(to_datetime64('2000-01-02 03:04:05.12', 2, 'UTC'), to_decimal64(10000, 0));
SELECT time_slots(to_datetime64('2000-01-02 03:04:05.233', 3, 'UTC'), to_decimal64(10000.12, 2), to_decimal64(634.1, 1));
SELECT time_slots(to_datetime64('2000-01-02 03:04:05.3456', 4, 'UTC'), to_decimal64(600, 0), to_decimal64(30, 0));

SELECT time_slots(to_datetime64('2000-01-02 03:04:05.23', 2, 'UTC')); -- { serverError 42 }
SELECT time_slots(to_datetime64('2000-01-02 03:04:05.345', 3, 'UTC'), to_decimal64(62.3, 1), to_decimal64(12.34, 2), 'one more'); -- { serverError 42 }
SELECT time_slots(to_datetime64('2000-01-02 03:04:05.456', 3, 'UTC'), 'wrong argument'); -- { serverError 43 }
SELECT time_slots(to_datetime64('2000-01-02 03:04:05.123', 3, 'UTC'), to_decimal64(600, 0), 'wrong argument'); -- { serverError 43 }
SELECT time_slots(to_datetime64('2000-01-02 03:04:05.1232', 4, 'UTC'), to_decimal64(600, 0), to_decimal64(0, 0)); -- { serverError 44 }