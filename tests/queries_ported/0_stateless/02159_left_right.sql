-- { echo }

SELECT left('Hello', 3);
SELECT left('Hello', -3);
SELECT left('Hello', 5);
SELECT left('Hello', -5);
SELECT left('Hello', 6);
SELECT left('Hello', -6);
SELECT left('Hello', 0);
SELECT left('Hello', NULL);

SELECT left(materialize('Привет'), 4);
SELECT LEFT('Привет', -4);
SELECT left(to_nullable('Привет'), 12);
SELECT lEFT('Привет', -12);
SELECT left(materialize(to_nullable('Привет')), 13);
SELECT left('Привет', -13);
SELECT Left('Привет', 0);
SELECT left('Привет', NULL);

SELECT left_utf8('Привет', 4);
SELECT left_utf8('Привет', -4);
SELECT left_utf8('Привет', 12);
SELECT left_utf8('Привет', -12);
SELECT left_utf8('Привет', 13);
SELECT left_utf8('Привет', -13);
SELECT left_utf8('Привет', 0);
SELECT left_utf8('Привет', NULL);

SELECT left('Hello', number) FROM numbers(10);
SELECT left_utf8('Привет', number) FROM numbers(10);
SELECT left('Hello', -number) FROM numbers(10);
SELECT left_utf8('Привет', -number) FROM numbers(10);

SELECT left_utf8('Привет', number % 3 = 0 ? NULL : (number % 2 ? to_int64(number) : -number)) FROM numbers(10);
SELECT left_utf8(number < 5 ? 'Hello' : 'Привет', number % 3 = 0 ? NULL : (number % 2 ? to_int64(number) : -number)) FROM numbers(10);

SELECT right('Hello', 3);
SELECT right('Hello', -3);
SELECT right('Hello', 5);
SELECT right('Hello', -5);
SELECT right('Hello', 6);
SELECT right('Hello', -6);
SELECT right('Hello', 0);
SELECT right('Hello', NULL);

SELECT RIGHT(materialize('Привет'), 4);
SELECT right('Привет', -4);
SELECT Right(to_nullable('Привет'), 12);
SELECT right('Привет', -12);
SELECT rIGHT(materialize(to_nullable('Привет')), 13);
SELECT right('Привет', -13);
SELECT rIgHt('Привет', 0);
SELECT RiGhT('Привет', NULL);

SELECT right_utf8('Привет', 4);
SELECT right_utf8('Привет', -4);
SELECT right_utf8('Привет', 12);
SELECT right_utf8('Привет', -12);
SELECT right_utf8('Привет', 13);
SELECT right_utf8('Привет', -13);
SELECT right_utf8('Привет', 0);
SELECT right_utf8('Привет', NULL);

SELECT right('Hello', number) FROM numbers(10);
SELECT right_utf8('Привет', number) FROM numbers(10);
SELECT right('Hello', -number) FROM numbers(10);
SELECT right_utf8('Привет', -number) FROM numbers(10);

SELECT right_utf8('Привет', number % 3 = 0 ? NULL : (number % 2 ? to_int64(number) : -number)) FROM numbers(10);
SELECT right_utf8(number < 5 ? 'Hello' : 'Привет', number % 3 = 0 ? NULL : (number % 2 ? to_int64(number) : -number)) FROM numbers(10);
