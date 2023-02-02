SELECT array_map(x -> array_join([x, 1]), [1, 2]); -- { serverError 36 }
