SELECT to_decimal32('e', 1); -- { serverError 72 }
SELECT to_decimal64('e', 2); -- { serverError 72 }
SELECT to_decimal128('e', 3); -- { serverError 72 }

SELECT to_decimal32OrNull('e', 1) x, is_null(x);
SELECT to_decimal64OrNull('e', 2) x, is_null(x);
SELECT to_decimal128OrNull('e', 3) x, is_null(x);
