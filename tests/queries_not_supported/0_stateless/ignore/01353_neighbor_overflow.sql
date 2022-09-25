SELECT neighbor(to_string(number), -9223372036854775808) FROM numbers(100); -- { serverError 69 }
WITH neighbor(to_string(number), to_int64(rand64())) AS x SELECT * FROM system.numbers WHERE NOT ignore(x); -- { serverError 69 }
