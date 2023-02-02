SELECT array_map(x -> x * sum(x), range(10)); -- { serverError 47 }
