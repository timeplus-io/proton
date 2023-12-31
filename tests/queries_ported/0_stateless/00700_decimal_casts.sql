SELECT to_decimal32('1.1', 1), to_decimal32('1.1', 2), to_decimal32('1.1', 8);
SELECT to_decimal32('1.1', 0);
SELECT to_decimal32(1.1, 0), to_decimal32(1.1, 1), to_decimal32(1.1, 2), to_decimal32(1.1, 8);

SELECT '1000000000' AS x, to_decimal32(x, 0); -- { serverError 69 }
SELECT '-1000000000' AS x, to_decimal32(x, 0); -- { serverError 69 }
SELECT '1000000000000000000' AS x, to_decimal64(x, 0); -- { serverError 69 }
SELECT '-1000000000000000000' AS x, to_decimal64(x, 0); -- { serverError 69 }
SELECT '100000000000000000000000000000000000000' AS x, to_decimal128(x, 0); -- { serverError 69 }
SELECT '-100000000000000000000000000000000000000' AS x, to_decimal128(x, 0); -- { serverError 69 }
SELECT '1' AS x, to_decimal32(x, 9); -- { serverError 69 }
SELECT '-1' AS x, to_decimal32(x, 9); -- { serverError 69 }
SELECT '1' AS x, to_decimal64(x, 18); -- { serverError 69 }
SELECT '-1' AS x, to_decimal64(x, 18); -- { serverError 69 }
SELECT '1' AS x, to_decimal128(x, 38); -- { serverError 69 }
SELECT '-1' AS x, to_decimal128(x, 38); -- { serverError 69 }

SELECT '0.1' AS x, to_decimal32(x, 0);
SELECT '-0.1' AS x, to_decimal32(x, 0);
SELECT '0.1' AS x, to_decimal64(x, 0);
SELECT '-0.1' AS x, to_decimal64(x, 0);
SELECT '0.1' AS x, to_decimal128(x, 0);
SELECT '-0.1' AS x, to_decimal128(x, 0);
SELECT '0.0000000001' AS x, to_decimal32(x, 9);
SELECT '-0.0000000001' AS x, to_decimal32(x, 9);
SELECT '0.0000000000000000001' AS x, to_decimal64(x, 18);
SELECT '-0.0000000000000000001' AS x, to_decimal64(x, 18);
SELECT '0.000000000000000000000000000000000000001' AS x, to_decimal128(x, 38);
SELECT '-0.000000000000000000000000000000000000001' AS x, to_decimal128(x, 38);

SELECT '1e9' AS x, to_decimal32(x, 0); -- { serverError 69 }
SELECT '-1E9' AS x, to_decimal32(x, 0); -- { serverError 69 }
SELECT '1E18' AS x, to_decimal64(x, 0); -- { serverError 69 }
SELECT '-1e18' AS x, to_decimal64(x, 0); -- { serverError 69 }
SELECT '1e38' AS x, to_decimal128(x, 0); -- { serverError 69 }
SELECT '-1E38' AS x, to_decimal128(x, 0); -- { serverError 69 }
SELECT '1e0' AS x, to_decimal32(x, 9); -- { serverError 69 }
SELECT '-1e-0' AS x, to_decimal32(x, 9); -- { serverError 69 }
SELECT '1e0' AS x, to_decimal64(x, 18); -- { serverError 69 }
SELECT '-1e-0' AS x, to_decimal64(x, 18); -- { serverError 69 }
SELECT '1e-0' AS x, to_decimal128(x, 38); -- { serverError 69 }
SELECT '-1e0' AS x, to_decimal128(x, 38); -- { serverError 69 }

SELECT '1e-1' AS x, to_decimal32(x, 0);
SELECT '-1e-1' AS x, to_decimal32(x, 0);
SELECT '1e-1' AS x, to_decimal64(x, 0);
SELECT '-1e-1' AS x, to_decimal64(x, 0);
SELECT '1e-1' AS x, to_decimal128(x, 0);
SELECT '-1e-1' AS x, to_decimal128(x, 0);
SELECT '1e-10' AS x, to_decimal32(x, 9);
SELECT '-1e-10' AS x, to_decimal32(x, 9);
SELECT '1e-19' AS x, to_decimal64(x, 18);
SELECT '-1e-19' AS x, to_decimal64(x, 18);
SELECT '1e-39' AS x, to_decimal128(x, 38);
SELECT '-1e-39' AS x, to_decimal128(x, 38);

SELECT to_float32(9999999)   as x, to_decimal32(x, 0), to_decimal32(-x, 0), to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_float32(999999.9)  as x, to_decimal32(x, 1), to_decimal32(-x, 1), to_decimal64(x, 1), to_decimal64(-x, 1);
SELECT to_float32(99999.99)  as x, to_decimal32(x, 2), to_decimal32(-x, 2), to_decimal64(x, 2), to_decimal64(-x, 2);
SELECT to_float32(9999.999)  as x, to_decimal32(x, 3), to_decimal32(-x, 3), to_decimal64(x, 3), to_decimal64(-x, 3);
SELECT to_float32(999.9999)  as x, to_decimal32(x, 4), to_decimal32(-x, 4), to_decimal64(x, 4), to_decimal64(-x, 4);
SELECT to_float32(99.99999)  as x, to_decimal32(x, 5), to_decimal32(-x, 5), to_decimal64(x, 5), to_decimal64(-x, 5);
SELECT to_float32(9.999999)  as x, to_decimal32(x, 6), to_decimal32(-x, 6), to_decimal64(x, 6), to_decimal64(-x, 6);
SELECT to_float32(0.9999999) as x, to_decimal32(x, 7), to_decimal32(-x, 7), to_decimal64(x, 7), to_decimal64(-x, 7);

SELECT to_float32(9.99999999)  as x, to_decimal32(x, 8), to_decimal32(-x, 8), to_decimal64(x, 8), to_decimal64(-x, 8);
SELECT to_float32(0.999999999) as x, to_decimal32(x, 9), to_decimal32(-x, 9), to_decimal64(x, 9), to_decimal64(-x, 9);

SELECT to_float64(999999999)   as x, to_decimal32(x, 0), to_decimal32(-x, 0), to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_float64(99999999.9)  as x, to_decimal32(x, 1), to_decimal32(-x, 1), to_decimal64(x, 1), to_decimal64(-x, 1);
SELECT to_float64(9999999.99)  as x, to_decimal32(x, 2), to_decimal32(-x, 2), to_decimal64(x, 2), to_decimal64(-x, 2);
SELECT to_float64(999999.999)  as x, to_decimal32(x, 3), to_decimal32(-x, 3), to_decimal64(x, 3), to_decimal64(-x, 3);
SELECT to_float64(99999.9999)  as x, to_decimal32(x, 4), to_decimal32(-x, 4), to_decimal64(x, 4), to_decimal64(-x, 4);
SELECT to_float64(9999.99999)  as x, to_decimal32(x, 5), to_decimal32(-x, 5), to_decimal64(x, 5), to_decimal64(-x, 5);
SELECT to_float64(999.999999)  as x, to_decimal32(x, 6), to_decimal32(-x, 6), to_decimal64(x, 6), to_decimal64(-x, 6);
SELECT to_float64(99.9999999)  as x, to_decimal32(x, 7), to_decimal32(-x, 7), to_decimal64(x, 7), to_decimal64(-x, 7);
SELECT to_float64(9.99999999)  as x, to_decimal32(x, 8), to_decimal32(-x, 8), to_decimal64(x, 8), to_decimal64(-x, 8);
SELECT to_float64(0.999999999) as x, to_decimal32(x, 9), to_decimal32(-x, 9), to_decimal64(x, 9), to_decimal64(-x, 9);

SELECT to_float64(999999999.999999999)  as x, to_decimal64(x, 9), to_decimal64(-x, 9);
SELECT to_float64(99999999.9999999999)  as x, to_decimal64(x, 10), to_decimal64(-x, 10);
SELECT to_float64(9999999.99999999999)  as x, to_decimal64(x, 11), to_decimal64(-x, 11);
SELECT to_float64(999999.999999999999)  as x, to_decimal64(x, 12), to_decimal64(-x, 12);
SELECT to_float64(99999.9999999999999)  as x, to_decimal64(x, 13), to_decimal64(-x, 13);
SELECT to_float64(9999.99999999999999)  as x, to_decimal64(x, 14), to_decimal64(-x, 14);
SELECT to_float64(999.999999999999999)  as x, to_decimal64(x, 15), to_decimal64(-x, 15);
SELECT to_float64(99.9999999999999999)  as x, to_decimal64(x, 16), to_decimal64(-x, 16);
SELECT to_float64(9.99999999999999999)  as x, to_decimal64(x, 17), to_decimal64(-x, 17);
SELECT to_float64(0.999999999999999999) as x, to_decimal64(x, 18), to_decimal64(-x, 18);

SELECT to_float64(999999999999999999)   as x, to_decimal128(x, 0), to_decimal128(-x, 0);
SELECT to_float64(99999999999999999.9)  as x, to_decimal128(x, 1), to_decimal128(-x, 1);
SELECT to_float64(9999999999999999.99)  as x, to_decimal128(x, 2), to_decimal128(-x, 2);
SELECT to_float64(999999999999999.999)  as x, to_decimal128(x, 3), to_decimal128(-x, 3);
SELECT to_float64(99999999999999.9999)  as x, to_decimal128(x, 4), to_decimal128(-x, 4);
SELECT to_float64(9999999999999.99999)  as x, to_decimal128(x, 5), to_decimal128(-x, 5);
SELECT to_float64(999999999999.999999)  as x, to_decimal128(x, 6), to_decimal128(-x, 6);
SELECT to_float64(99999999999.9999999)  as x, to_decimal128(x, 7), to_decimal128(-x, 7);
SELECT to_float64(9999999999.99999999)  as x, to_decimal128(x, 8), to_decimal128(-x, 8);
SELECT to_float64(999999999.999999999)  as x, to_decimal128(x, 9), to_decimal128(-x, 9);
SELECT to_float64(999999999.999999999)  as x, to_decimal128(x, 9), to_decimal128(-x, 9);
SELECT to_float64(99999999.9999999999)  as x, to_decimal128(x, 10), to_decimal128(-x, 10);
SELECT to_float64(9999999.99999999999)  as x, to_decimal128(x, 11), to_decimal128(-x, 11);
SELECT to_float64(999999.999999999999)  as x, to_decimal128(x, 12), to_decimal128(-x, 12);
SELECT to_float64(99999.9999999999999)  as x, to_decimal128(x, 13), to_decimal128(-x, 13);
SELECT to_float64(9999.99999999999999)  as x, to_decimal128(x, 14), to_decimal128(-x, 14);
SELECT to_float64(999.999999999999999)  as x, to_decimal128(x, 15), to_decimal128(-x, 15);
SELECT to_float64(99.9999999999999999)  as x, to_decimal128(x, 16), to_decimal128(-x, 16);
SELECT to_float64(9.99999999999999999)  as x, to_decimal128(x, 17), to_decimal128(-x, 17);
SELECT to_float64(0.999999999999999999) as x, to_decimal128(x, 18), to_decimal128(-x, 18);

SELECT to_decimal32(number, 4) as n1, to_decimal32(n1 / 9, 2) as n2, to_decimal32(n2, 8) FROM system.numbers LIMIT 10;
SELECT to_decimal32(number, 4) as n1, to_decimal32(n1 / 9, 8) as n2, to_decimal32(n2, 2) FROM system.numbers LIMIT 10;
SELECT to_decimal32(number, 8) as n1, to_decimal32(n1 / 9, 4) as n2, to_decimal32(n2, 2) FROM system.numbers LIMIT 10;

SELECT to_decimal64(number, 4) as n1, to_decimal64(n1 / 9, 2) as n2, to_decimal64(n2, 8) FROM system.numbers LIMIT 10;
SELECT to_decimal64(number, 4) as n1, to_decimal64(n1 / 9, 8) as n2, to_decimal64(n2, 2) FROM system.numbers LIMIT 10;
SELECT to_decimal64(number, 8) as n1, to_decimal64(n1 / 9, 4) as n2, to_decimal64(n2, 2) FROM system.numbers LIMIT 10;

SELECT to_int8(99) as x, to_decimal32(x, 0), to_decimal32(-x, 0), to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_int16(9999) as x, to_decimal32(x, 0), to_decimal32(-x, 0), to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_int32(999999999) as x, to_decimal32(x, 0), to_decimal32(-x, 0), to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_int64(999999999) as x, to_decimal32(x, 0), to_decimal32(-x, 0), to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_int32(999999999) as x, to_decimal64(x, 9), to_decimal64(-x, 9), to_decimal128(x, 29), to_decimal128(-x, 29);
SELECT to_int64(999999999) as x, to_decimal64(x, 9), to_decimal64(-x, 9), to_decimal128(x, 29), to_decimal128(-x, 29);
SELECT to_int64(999999999999999999) as x, to_decimal64(x, 0), to_decimal64(-x, 0);
SELECT to_int64(999999999999999999) as x, to_decimal128(x, 0), to_decimal128(-x, 0);
SELECT to_int64(999999999999999999) as x, to_decimal128(x, 20), to_decimal128(-x, 20);

SELECT to_uint8(99) as x, to_decimal32(x, 0), to_decimal64(x, 0);
SELECT to_uint16(9999) as x, to_decimal32(x, 0), to_decimal64(x, 0);
SELECT to_uint32(999999999) as x, to_decimal32(x, 0), to_decimal64(x, 0);
SELECT to_uint64(999999999) as x, to_decimal32(x, 0), to_decimal64(x, 0);

SELECT CAST('42.4200', 'Decimal(9,2)') AS a, CAST(a, 'Decimal(9,2)'), CAST(a, 'Decimal(18, 2)'), CAST(a, 'Decimal(38, 2)');
SELECT CAST('42.42', 'Decimal(9,2)') AS a, CAST(a, 'Decimal(9,7)'), CAST(a, 'Decimal(18, 16)'), CAST(a, 'Decimal(38, 36)');

SELECT CAST('123456789', 'Decimal(9,0)'), CAST('123456789123456789', 'Decimal(18,0)');
SELECT CAST('12345678901234567890123456789012345678', 'Decimal(38,0)');
SELECT CAST('123456789', 'Decimal(9,1)'); -- { serverError 69 }
SELECT CAST('123456789123456789', 'Decimal(18,1)'); -- { serverError 69 }
SELECT CAST('12345678901234567890123456789012345678', 'Decimal(38,1)'); -- { serverError 69 }

SELECT CAST('0.123456789', 'Decimal(9,9)'), CAST('0.123456789123456789', 'Decimal(18,18)');
SELECT CAST('0.12345678901234567890123456789012345678', 'Decimal(38,38)');
SELECT CAST('0.123456789', 'Decimal(9,8)');
SELECT CAST('0.123456789123456789', 'Decimal(18,17)');
SELECT CAST('0.12345678901234567890123456789012345678', 'Decimal(38,37)');
