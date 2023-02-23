SELECT to_decimal32_or_default(111, 3, 123.456::decimal32(3)) AS x, to_type_name(x);
SELECT to_decimal64_or_default(222, 3, 123.456::decimal64(3)) AS x, to_type_name(x);
SELECT to_decimal128_or_default(333, 3, 123.456::decimal128(3)) AS x, to_type_name(x);
SELECT to_decimal256_or_default(444, 3, 123.456::decimal256(3)) AS x, to_type_name(x);

SELECT to_decimal32_or_default('Hello', 3, 123.456::decimal32(3)) AS x, to_type_name(x);
SELECT to_decimal64_or_default('Hello', 3, 123.456::decimal64(3)) AS x, to_type_name(x);
SELECT to_decimal128_or_default('Hello', 3, 123.456::decimal128(3)) AS x, to_type_name(x);
SELECT to_decimal256_or_default('Hello', 3, 123.456::decimal256(3)) AS x, to_type_name(x);
