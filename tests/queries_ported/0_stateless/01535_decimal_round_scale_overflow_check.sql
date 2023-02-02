SELECT round(to_decimal32(1, 0), -9223372036854775806); -- { serverError 69 }
