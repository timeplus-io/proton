SELECT array_reduce('agg_throw(0.0001)', range(number % 10)) FROM system.numbers FORMAT Null; -- { serverError 503 }
