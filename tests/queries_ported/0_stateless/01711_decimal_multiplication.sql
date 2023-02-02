SELECT materialize(to_decimal64(4,4)) - materialize(to_decimal32(2,2));
SELECT to_decimal64(4,4) - materialize(to_decimal32(2,2));
SELECT materialize(to_decimal64(4,4)) - to_decimal32(2,2);
SELECT to_decimal64(4,4) - to_decimal32(2,2);
