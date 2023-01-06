SELECT time_slot(to_datetime('2000-01-02 03:04:05', 'UTC'));
SELECT time_slots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(10000));
SELECT time_slots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(10000), 600);
SELECT time_slots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(600), 30);
SELECT time_slots(to_datetime('2000-01-02 03:04:05', 'UTC'), 'wrong argument'); -- { serverError 43 }
SELECT time_slots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(600), 'wrong argument'); -- { serverError 43 }
SELECT time_slots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(600), 0); -- { serverError 44 }