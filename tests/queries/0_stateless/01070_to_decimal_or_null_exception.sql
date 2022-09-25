SELECT to_decimal32('e', 1); -- { serverError 72 }
SELECT to_decimal64('e', 2); -- { serverError 72 }
SELECT toDecimal128('e', 3); -- { serverError 72 }

SELECT toDecimal32OrNull('e', 1) x, is_null(x);
SELECT toDecimal64OrNull('e', 2) x, is_null(x);
SELECT toDecimal128OrNull('e', 3) x, is_null(x);
