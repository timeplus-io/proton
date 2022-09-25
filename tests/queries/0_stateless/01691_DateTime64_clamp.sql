-- { echo }
-- These values are within the extended range of DateTime64 [1925-01-01, 2284-01-01)
SELECT to_timezone(to_datetime(-2, 2), 'Europe/Moscow');
SELECT toDateTime64(-2, 2, 'Europe/Moscow');
SELECT CAST(-1 AS DateTime64(0, 'Europe/Moscow'));
SELECT CAST('2020-01-01 00:00:00.3' AS DateTime64(0, 'Europe/Moscow'));
SELECT toDateTime64(bit_shift_left(to_uint64(1), 33), 2, 'Europe/Moscow') FORMAT Null;
SELECT to_timezone(to_datetime(-2., 2), 'Europe/Moscow');
SELECT toDateTime64(-2., 2, 'Europe/Moscow');
SELECT toDateTime64(to_float32(bit_shift_left(to_uint64(1),33)), 2, 'Europe/Moscow');
SELECT toDateTime64(to_float64(bit_shift_left(to_uint64(1),33)), 2, 'Europe/Moscow') FORMAT Null;

-- These are outsize of extended range and hence clamped
SELECT toDateTime64(-1 * bit_shift_left(to_uint64(1), 35), 2, 'Europe/Moscow');
SELECT CAST(-1 * bit_shift_left(to_uint64(1), 35) AS DateTime64(3, 'Europe/Moscow'));
SELECT CAST(bit_shift_left(to_uint64(1), 35) AS DateTime64(3, 'Europe/Moscow'));
SELECT toDateTime64(bit_shift_left(to_uint64(1), 35), 2, 'Europe/Moscow');
