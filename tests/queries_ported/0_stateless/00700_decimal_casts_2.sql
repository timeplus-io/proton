SELECT to_decimal128('1234567890', 28) AS x, to_decimal128(x, 29), to_decimal128(to_decimal128('1234567890', 28), 29);
SELECT to_decimal128(to_decimal128('1234567890', 28), 30);

SELECT to_decimal64('1234567890', 8) AS x, to_decimal64(x, 9), to_decimal64(to_decimal64('1234567890', 8), 9);
SELECT to_decimal64(to_decimal64('1234567890', 8), 10); -- { serverError 407 }

SELECT to_decimal32('12345678', 1) AS x, to_decimal32(x, 2), to_decimal32(to_decimal32('12345678', 1), 2);
SELECT to_decimal32(to_decimal32('12345678', 1), 3); -- { serverError 407 }

SELECT to_decimal64(to_decimal64('92233720368547758.1', 1), 2); -- { serverError 407 }
SELECT to_decimal64(to_decimal64('-92233720368547758.1', 1), 2); -- { serverError 407 }

SELECT to_decimal128('9223372036854775807', 6) AS x, to_int64(x), to_int64(-x);
SELECT to_decimal128('9223372036854775809', 6) AS x, to_int64(x); -- { serverError 407 }
SELECT to_decimal128('9223372036854775809', 6) AS x, to_int64(-x); -- { serverError 407 }
SELECT to_decimal64('922337203685477580', 0) * 10 AS x, to_int64(x), to_int64(-x);
SELECT to_decimal64(to_decimal64('92233720368547758.0', 1), 2) AS x, to_int64(x), to_int64(-x);

SELECT to_decimal128('2147483647', 10) AS x, to_int32(x), to_int32(-x);
SELECT to_decimal128('2147483649', 10) AS x, to_int32(x), to_int32(-x); -- { serverError 407 }
SELECT to_decimal64('2147483647', 2) AS x, to_int32(x), to_int32(-x);
SELECT to_decimal64('2147483649', 2) AS x, to_int32(x), to_int32(-x); -- { serverError 407 }

SELECT to_decimal128('92233720368547757.99', 2) AS x, to_int64(x), to_int64(-x);
SELECT to_decimal64('2147483640.99', 2) AS x, to_int32(x), to_int32(-x);

SELECT to_decimal128('-0.9', 8) AS x, to_uint64(x);
SELECT to_decimal64('-0.9', 8) AS x, to_uint64(x);
SELECT to_decimal32('-0.9', 8) AS x, to_uint64(x);

SELECT to_decimal128('-0.8', 4) AS x, to_uint32(x);
SELECT to_decimal64('-0.8', 4) AS x, to_uint32(x);
SELECT to_decimal32('-0.8', 4) AS x, to_uint32(x);

SELECT to_decimal128('-0.7', 2) AS x, to_uint16(x);
SELECT to_decimal64('-0.7', 2) AS x, to_uint16(x);
SELECT to_decimal32('-0.7', 2) AS x, to_uint16(x);

SELECT to_decimal128('-0.6', 6) AS x, to_uint8(x);
SELECT to_decimal64('-0.6', 6) AS x, to_uint8(x);
SELECT to_decimal32('-0.6', 6) AS x, to_uint8(x);

SELECT to_decimal128('-1', 7) AS x, to_uint64(x); -- { serverError 407 }
SELECT to_decimal128('-1', 7) AS x, to_uint32(x); -- { serverError 407 }
SELECT to_decimal128('-1', 7) AS x, to_uint16(x); -- { serverError 407 }
SELECT to_decimal128('-1', 7) AS x, to_uint8(x); -- { serverError 407 }

SELECT to_decimal64('-1', 5) AS x, to_uint64(x); -- { serverError 407 }
SELECT to_decimal64('-1', 5) AS x, to_uint32(x); -- { serverError 407 }
SELECT to_decimal64('-1', 5) AS x, to_uint16(x); -- { serverError 407 }
SELECT to_decimal64('-1', 5) AS x, to_uint8(x); -- { serverError 407 }

SELECT to_decimal32('-1', 3) AS x, to_uint64(x); -- { serverError 407 }
SELECT to_decimal32('-1', 3) AS x, to_uint32(x); -- { serverError 407 }
SELECT to_decimal32('-1', 3) AS x, to_uint16(x); -- { serverError 407 }
SELECT to_decimal32('-1', 3) AS x, to_uint8(x); -- { serverError 407 }

SELECT to_decimal128('18446744073709551615', 0) AS x, to_uint64(x);
SELECT to_decimal128('18446744073709551616', 0) AS x, to_uint64(x); -- { serverError 407 }
SELECT to_decimal128('18446744073709551615', 8) AS x, to_uint64(x);
SELECT to_decimal128('18446744073709551616', 8) AS x, to_uint64(x); -- { serverError 407 }

SELECT to_decimal128('4294967295', 0) AS x, to_uint32(x);
SELECT to_decimal128('4294967296', 0) AS x, to_uint32(x); -- { serverError 407 }
SELECT to_decimal128('4294967295', 10) AS x, to_uint32(x);
SELECT to_decimal128('4294967296', 10) AS x, to_uint32(x); -- { serverError 407 }
SELECT to_decimal64('4294967295', 0) AS x, to_uint32(x);
SELECT to_decimal64('4294967296', 0) AS x, to_uint32(x); -- { serverError 407 }
SELECT to_decimal64('4294967295', 4) AS x, to_uint32(x);
SELECT to_decimal64('4294967296', 4) AS x, to_uint32(x); -- { serverError 407 }

SELECT to_decimal128('65535', 0) AS x, to_uint16(x);
SELECT to_decimal128('65536', 0) AS x, to_uint16(x); -- { serverError 407 }
SELECT to_decimal128('65535', 10) AS x, to_uint16(x);
SELECT to_decimal128('65536', 10) AS x, to_uint16(x); -- { serverError 407 }
SELECT to_decimal64('65535', 0) AS x, to_uint16(x);
SELECT to_decimal64('65536', 0) AS x, to_uint16(x); -- { serverError 407 }
SELECT to_decimal64('65535', 4) AS x, to_uint16(x);
SELECT to_decimal64('65536', 4) AS x, to_uint16(x); -- { serverError 407 }

SELECT to_int64('2147483647') AS x, to_decimal32(x, 0);
SELECT to_int64('-2147483647') AS x, to_decimal32(x, 0);
SELECT to_uint64('2147483647') AS x, to_decimal32(x, 0);
SELECT to_int64('2147483649') AS x, to_decimal32(x, 0); -- { serverError 407 }
SELECT to_int64('-2147483649') AS x, to_decimal32(x, 0); -- { serverError 407 }
SELECT to_uint64('2147483649') AS x, to_decimal32(x, 0); -- { serverError 407 }

SELECT to_uint64('9223372036854775807') AS x, to_decimal64(x, 0);
SELECT to_uint64('9223372036854775809') AS x, to_decimal64(x, 0); -- { serverError 407 }

SELECT to_decimal32(0, row_number_in_block()); -- { serverError 44 }
SELECT to_decimal64(0, row_number_in_block()); -- { serverError 44 }
SELECT to_decimal128(0, row_number_in_block()); -- { serverError 44 }

SELECT to_decimal32(1/0, 0); -- { serverError 407 }
SELECT to_decimal64(1/0, 1); -- { serverError 407 }
SELECT to_decimal128(0/0, 2); -- { serverError 407 }
SELECT CAST(1/0, 'decimal(9, 0)'); -- { serverError 407 }
SELECT CAST(1/0, 'decimal(18, 1)'); -- { serverError 407 }
SELECT CAST(1/0, 'decimal(38, 2)'); -- { serverError 407 }
SELECT CAST(0/0, 'decimal(9, 3)'); -- { serverError 407 }
SELECT CAST(0/0, 'decimal(18, 4)'); -- { serverError 407 }
SELECT CAST(0/0, 'decimal(38, 5)'); -- { serverError 407 }

select to_decimal32(10000.1, 6); -- { serverError 407 }
select to_decimal64(10000.1, 18); -- { serverError 407 }
select to_decimal128(1000000000000000000000.1, 18); -- { serverError 407 }

select to_decimal32(-10000.1, 6); -- { serverError 407 }
select to_decimal64(-10000.1, 18); -- { serverError 407 }
select to_decimal128(-1000000000000000000000.1, 18); -- { serverError 407 }

select to_decimal32(2147483647.0 + 1.0, 0); -- { serverError 407 }
select to_decimal64(9223372036854775807.0, 0); -- { serverError 407 }
select to_decimal128(170141183460469231731687303715884105729.0, 0); -- { serverError 407 }

select to_decimal32(-2147483647.0 - 1.0, 0); -- { serverError 407 }
select to_decimal64(-9223372036854775807.0, 0); -- { serverError 407 }
select to_decimal128(-170141183460469231731687303715884105729.0, 0); -- { serverError 407 }
