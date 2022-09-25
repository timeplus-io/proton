SELECT countDigits(to_decimal32(0, 0)), countDigits(to_decimal32(42, 0)), countDigits(to_decimal32(4.2, 1)),
       countDigits(to_decimal64(0, 0)), countDigits(to_decimal64(42, 0)), countDigits(to_decimal64(4.2, 2)),
       countDigits(toDecimal128(0, 0)), countDigits(toDecimal128(42, 0)), countDigits(toDecimal128(4.2, 3));

SELECT countDigits(materialize(to_decimal32(4.2, 1))),
       countDigits(materialize(to_decimal64(4.2, 2))),
       countDigits(materialize(toDecimal128(4.2, 3)));

SELECT countDigits(to_decimal32(1, 9)), countDigits(to_decimal32(-1, 9)),
       countDigits(to_decimal64(1, 18)), countDigits(to_decimal64(-1, 18)),
       countDigits(toDecimal128(1, 38)), countDigits(toDecimal128(-1, 38));
       
SELECT countDigits(to_int8(42)), countDigits(to_int8(-42)), countDigits(to_uint8(42)),
       countDigits(to_int16(42)), countDigits(to_int16(-42)), countDigits(to_uint16(42)),
       countDigits(to_int32(42)), countDigits(to_int32(-42)), countDigits(to_uint32(42)),
       countDigits(to_int64(42)), countDigits(to_int64(-42)), countDigits(to_uint64(42));

SELECT countDigits(to_int8(0)), countDigits(to_int8(0)),  countDigits(to_uint8(0)),
       countDigits(to_int16(0)), countDigits(to_int16(0)), countDigits(to_uint16(0)),
       countDigits(to_int32(0)), countDigits(to_int32(0)), countDigits(to_uint32(0)),
       countDigits(to_int64(0)), countDigits(to_int64(0)), countDigits(to_uint64(0));
       
SELECT countDigits(to_int8(127)), countDigits(to_int8(-128)),  countDigits(to_uint8(255)),
       countDigits(to_int16(32767)), countDigits(to_int16(-32768)), countDigits(to_uint16(65535)),
       countDigits(to_int32(2147483647)), countDigits(to_int32(-2147483648)), countDigits(to_uint32(4294967295)),
       countDigits(to_int64(9223372036854775807)), countDigits(to_int64(-9223372036854775808)), countDigits(to_uint64(18446744073709551615));

SELECT countDigits(to_nullable(to_decimal32(4.2, 1))), countDigits(materialize(to_nullable(to_decimal32(4.2, 2)))),
       countDigits(to_nullable(to_decimal64(4.2, 3))), countDigits(materialize(to_nullable(to_decimal64(4.2, 4)))),
       countDigits(to_nullable(toDecimal128(4.2, 5))), countDigits(materialize(to_nullable(toDecimal128(4.2, 6))));
