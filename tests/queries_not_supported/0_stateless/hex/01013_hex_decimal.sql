SELECT hex(to_decimal32(1.0, 2));
SELECT hex(to_decimal32(1., 2));
SELECT hex(to_decimal32(0.000578, 6));
SELECT hex(to_decimal64(-123.978, 3));
SELECT hex(toDecimal128(99.67, 2));
SELECT hex(to_decimal32(number, 3)) FROM numbers(200, 2);
SELECT hex(to_decimal64(number, 5)) FROM numbers(202, 2);
SELECT hex(toDecimal128(number, 9)) FROM numbers(120, 2);
