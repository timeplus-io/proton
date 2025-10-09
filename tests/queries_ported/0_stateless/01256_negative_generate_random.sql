SELECT * FROM generate_random('i8', 1, 10, 10); -- { serverError 62 }
SELECT * FROM generate_random; -- { serverError 60 }
SELECT * FROM generate_random(); -- { serverError 42 }
SELECT * FROM generate_random('i8 uint8', 1, 10, 10, 10, 10); -- { serverError 42 }
SELECT * FROM generate_random('', 1, 10, 10); -- { serverError 62 }
