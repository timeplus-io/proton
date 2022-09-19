SELECT transform(1, [1], [to_decimal32(1, 2)]); -- { serverError 44 }
SELECT transform(to_decimal32(number, 2), [to_decimal32(3, 2)], [to_decimal32(30, 2)]) FROM system.numbers LIMIT 10;
SELECT transform(to_decimal32(number, 2), [to_decimal32(3, 2)], [to_decimal32(30, 2)], to_decimal32(1000, 2)) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 11], [to_decimal32(30, 2), to_decimal32(50, 2), to_decimal32(70,2)], to_decimal32(1000, 2)) FROM system.numbers LIMIT 10;
SELECT transform(number, [3, 5, 11], [to_decimal32(30, 2), to_decimal32(50, 2), to_decimal32(70,2)], to_decimal32(1000, 2)) FROM system.numbers LIMIT 10;
SELECT transform(to_string(number), ['3', '5', '7'], [to_decimal32(30, 2), to_decimal32(50, 2), to_decimal32(70,2)], to_decimal32(1000, 2)) FROM system.numbers LIMIT 10;





