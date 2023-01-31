SELECT to_decimal32('e', 1); -- { serverError 72 }
SELECT to_decimal64('e', 2); -- { serverError 72 }
SELECT to_decimal128('e', 3); -- { serverError 72 }

SELECT to_decimal32_or_null('e', 1) as x, is_null(x);
SELECT to_decimal64_or_null('e', 2) as x, is_null(x);
SELECT to_decimal128_or_null('e', 3) as x, is_null(x);
