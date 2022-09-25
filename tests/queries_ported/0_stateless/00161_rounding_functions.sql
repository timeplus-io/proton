SELECT to_uint8(number) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_uint16(number) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_uint32(number) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_uint64(number) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_int8(number - 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_int16(number - 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_int32(number - 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_int64(number - 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_float32(number - 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_float64(number - 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;

SELECT to_float32((number - 10) / 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;
SELECT to_float64((number - 10) / 10) AS x, round(x), round_bankers(x), floor(x), ceil(x), trunc(x) FROM system.numbers LIMIT 20;

SELECT to_float32((number - 10) / 10) AS x, round(x, 1), round_bankers(x, 1), floor(x, 1), ceil(x, 1), trunc(x, 1) FROM system.numbers LIMIT 20;
SELECT to_float64((number - 10) / 10) AS x, round(x, 1), round_bankers(x, 1), floor(x, 1), ceil(x, 1), trunc(x, 1) FROM system.numbers LIMIT 20;

SELECT to_uint8(number) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_uint16(number) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_uint32(number) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_uint64(number) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_int8(number - 10) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_int16(number - 10) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_int32(number - 10) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_int64(number - 10) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_float32(number - 10) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;
SELECT to_float64(number - 10) AS x, round(x, -1), round_bankers(x, -1), floor(x, -1), ceil(x, -1), trunc(x, -1) FROM system.numbers LIMIT 20;

SELECT to_uint8(number) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_uint16(number) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_uint32(number) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_uint64(number) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_int8(number - 10) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_int16(number - 10) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_int32(number - 10) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_int64(number - 10) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_float32(number - 10) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;
SELECT to_float64(number - 10) AS x, round(x, -2), round_bankers(x, -2), floor(x, -2), ceil(x, -2), trunc(x, -2) FROM system.numbers LIMIT 20;

SELECT 123456789 AS x, floor(x, -1), floor(x, -2), floor(x, -3), floor(x, -4), floor(x, -5), floor(x, -6), floor(x, -7), floor(x, -8), floor(x, -9), floor(x, -10);
SELECT 12345.6789 AS x, floor(x, -1), floor(x, -2), floor(x, -3), floor(x, -4), floor(x, -5), floor(x, 1), floor(x, 2), floor(x, 3), floor(x, 4), floor(x, 5);


SELECT round_to_exp2(100), round_to_exp2(64), round_to_exp2(3), round_to_exp2(0), round_to_exp2(-1);
SELECT round_to_exp2(0.9), round_to_exp2(0), round_to_exp2(-0.5), round_to_exp2(-0.6), round_to_exp2(-0.2);

SELECT ceil(29375422, -54212) --{serverError 69}
