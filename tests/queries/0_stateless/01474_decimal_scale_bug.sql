SELECT to_decimal32(1, 2) * to_decimal32(1, 1) x, to_type_name(x);
SELECT to_decimal32(1, 1) * to_decimal32(1, 2) x, to_type_name(x);
SELECT to_decimal32(1, 3) * to_decimal64(1, 1) x, to_type_name(x);
SELECT to_decimal32(1, 1) * to_decimal64(1, 3) x, to_type_name(x);
SELECT to_decimal32(1, 2) * toDecimal128(1, 3) x, to_type_name(x);
SELECT to_decimal32(1, 3) * toDecimal128(1, 2) x, to_type_name(x);

SELECT to_decimal64(1, 2) * to_decimal32(1, 1) x, to_type_name(x);
SELECT to_decimal64(1, 1) * to_decimal32(1, 2) x, to_type_name(x);
SELECT to_decimal64(1, 3) * to_decimal64(1, 1) x, to_type_name(x);
SELECT to_decimal64(1, 1) * to_decimal64(1, 3) x, to_type_name(x);
SELECT to_decimal64(1, 2) * toDecimal128(1, 3) x, to_type_name(x);
SELECT to_decimal64(1, 3) * toDecimal128(1, 2) x, to_type_name(x);

SELECT toDecimal128(1, 2) * to_decimal32(1, 1) x, to_type_name(x);
SELECT toDecimal128(1, 1) * to_decimal32(1, 2) x, to_type_name(x);
SELECT toDecimal128(1, 3) * to_decimal64(1, 1) x, to_type_name(x);
SELECT toDecimal128(1, 1) * to_decimal64(1, 3) x, to_type_name(x);
SELECT toDecimal128(1, 2) * toDecimal128(1, 3) x, to_type_name(x);
SELECT toDecimal128(1, 3) * toDecimal128(1, 2) x, to_type_name(x);
