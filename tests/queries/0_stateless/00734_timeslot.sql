SELECT timeSlot(to_datetime('2000-01-02 03:04:05', 'UTC'));
SELECT timeSlots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(10000));
SELECT timeSlots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(10000), 600);
SELECT timeSlots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(600), 30);
SELECT timeSlots(to_datetime('2000-01-02 03:04:05', 'UTC'), 'wrong argument'); -- { serverError 43 }
SELECT timeSlots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(600), 'wrong argument'); -- { serverError 43 }
SELECT timeSlots(to_datetime('2000-01-02 03:04:05', 'UTC'), to_uint32(600), 0); -- { serverError 44 }