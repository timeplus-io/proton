SELECT to_decimal32(1, 2) * to_decimal32(1, 1)as x, to_type_name(x);
SELECT to_decimal32(1, 1) * to_decimal32(1, 2)as x, to_type_name(x);
SELECT to_decimal32(1, 3) * to_decimal64(1, 1)as x, to_type_name(x);
SELECT to_decimal32(1, 1) * to_decimal64(1, 3)as x, to_type_name(x);
SELECT to_decimal32(1, 2) * to_decimal128(1, 3)as x, to_type_name(x);
SELECT to_decimal32(1, 3) * to_decimal128(1, 2)as x, to_type_name(x);

SELECT to_decimal64(1, 2) * to_decimal32(1, 1)as x, to_type_name(x);
SELECT to_decimal64(1, 1) * to_decimal32(1, 2)as x, to_type_name(x);
SELECT to_decimal64(1, 3) * to_decimal64(1, 1)as x, to_type_name(x);
SELECT to_decimal64(1, 1) * to_decimal64(1, 3)as x, to_type_name(x);
SELECT to_decimal64(1, 2) * to_decimal128(1, 3)as x, to_type_name(x);
SELECT to_decimal64(1, 3) * to_decimal128(1, 2)as x, to_type_name(x);

SELECT to_decimal128(1, 2) * to_decimal32(1, 1)as x, to_type_name(x);
SELECT to_decimal128(1, 1) * to_decimal32(1, 2)as x, to_type_name(x);
SELECT to_decimal128(1, 3) * to_decimal64(1, 1)as x, to_type_name(x);
SELECT to_decimal128(1, 1) * to_decimal64(1, 3)as x, to_type_name(x);
SELECT to_decimal128(1, 2) * to_decimal128(1, 3)as x, to_type_name(x);
SELECT to_decimal128(1, 3) * to_decimal128(1, 2)as x, to_type_name(x);
