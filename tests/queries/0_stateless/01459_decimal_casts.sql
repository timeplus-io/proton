SELECT to_uint32(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(1);
SELECT to_int32(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(1, 1);
SELECT to_int64(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(2, 1);
SELECT to_uint64(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(3, 1);
SELECT to_int128(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(4, 1);
SELECT toInt256(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(5, 1);
SELECT toUInt256(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(6, 1);
SELECT to_float32(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(7, 1);
SELECT toFloat64(number) y, to_decimal32(y, 1), to_decimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(8, 1);

SELECT to_int32(to_decimal32(number, 1)), to_int64(to_decimal32(number, 1)), to_int128(to_decimal32(number, 1)) FROM numbers(9, 1);
SELECT to_int32(to_decimal64(number, 2)), to_int64(to_decimal64(number, 2)), to_int128(to_decimal64(number, 2)) FROM numbers(10, 1);
SELECT to_int32(toDecimal128(number, 3)), to_int64(toDecimal128(number, 3)), to_int128(toDecimal128(number, 3)) FROM numbers(11, 1);
SELECT to_float32(to_decimal32(number, 1)), to_float32(to_decimal64(number, 2)), to_float32(toDecimal128(number, 3)) FROM numbers(12, 1);
SELECT toFloat64(to_decimal32(number, 1)), toFloat64(to_decimal64(number, 2)), toFloat64(toDecimal128(number, 3)) FROM numbers(13, 1);
SELECT toInt256(to_decimal32(number, 1)), toInt256(to_decimal64(number, 2)), toInt256(toDecimal128(number, 3)) FROM numbers(14, 1);
