SELECT DISTINCT int_div(number, nan) FROM numbers(10); -- { serverError 153 }
