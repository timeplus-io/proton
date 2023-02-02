SELECT count_digits(to_decimal32(0, 0)), count_digits(to_decimal32(42, 0)), count_digits(to_decimal32(4.2, 1)),
       count_digits(to_decimal64(0, 0)), count_digits(to_decimal64(42, 0)), count_digits(to_decimal64(4.2, 2)),
       count_digits(to_decimal128(0, 0)), count_digits(to_decimal128(42, 0)), count_digits(to_decimal128(4.2, 3));

SELECT count_digits(materialize(to_decimal32(4.2, 1))),
       count_digits(materialize(to_decimal64(4.2, 2))),
       count_digits(materialize(to_decimal128(4.2, 3)));

SELECT count_digits(to_decimal32(1, 9)), count_digits(to_decimal32(-1, 9)),
       count_digits(to_decimal64(1, 18)), count_digits(to_decimal64(-1, 18)),
       count_digits(to_decimal128(1, 38)), count_digits(to_decimal128(-1, 38));
       
SELECT count_digits(to_int8(42)), count_digits(to_int8(-42)), count_digits(to_uint8(42)),
       count_digits(to_int16(42)), count_digits(to_int16(-42)), count_digits(to_uint16(42)),
       count_digits(to_int32(42)), count_digits(to_int32(-42)), count_digits(to_uint32(42)),
       count_digits(to_int64(42)), count_digits(to_int64(-42)), count_digits(to_uint64(42));

SELECT count_digits(to_int8(0)), count_digits(to_int8(0)),  count_digits(to_uint8(0)),
       count_digits(to_int16(0)), count_digits(to_int16(0)), count_digits(to_uint16(0)),
       count_digits(to_int32(0)), count_digits(to_int32(0)), count_digits(to_uint32(0)),
       count_digits(to_int64(0)), count_digits(to_int64(0)), count_digits(to_uint64(0));
       
SELECT count_digits(to_int8(127)), count_digits(to_int8(-128)),  count_digits(to_uint8(255)),
       count_digits(to_int16(32767)), count_digits(to_int16(-32768)), count_digits(to_uint16(65535)),
       count_digits(to_int32(2147483647)), count_digits(to_int32(-2147483648)), count_digits(to_uint32(4294967295)),
       count_digits(to_int64(9223372036854775807)), count_digits(to_int64(-9223372036854775808)), count_digits(to_uint64(18446744073709551615));

SELECT count_digits(to_nullable(to_decimal32(4.2, 1))), count_digits(materialize(to_nullable(to_decimal32(4.2, 2)))),
       count_digits(to_nullable(to_decimal64(4.2, 3))), count_digits(materialize(to_nullable(to_decimal64(4.2, 4)))),
       count_digits(to_nullable(to_decimal128(4.2, 5))), count_digits(materialize(to_nullable(to_decimal128(4.2, 6))));
