SELECT array_resize([1, 2, 3], -9223372036854775808); -- { serverError 128 }
