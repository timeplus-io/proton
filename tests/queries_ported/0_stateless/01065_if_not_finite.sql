SELECT if_not_finite(round(1 / number, 2), 111) FROM numbers(10);

SELECT if_not_finite(1, 2);
SELECT if_not_finite(-1.0, 2);
SELECT if_not_finite(nan, 2);
SELECT if_not_finite(-1 / 0, 2);
SELECT if_not_finite(log(0), NULL);
SELECT if_not_finite(sqrt(-1), -42);
SELECT if_not_finite(1234567890123456789, -1234567890123456789); -- { serverError 386 }

SELECT if_not_finite(NULL, 1);
