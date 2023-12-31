SELECT substring_utf8('hello, привет', 1, number) FROM numbers(16);
SELECT substring_utf8('hello, привет', number + 1, 3) FROM numbers(16);
SELECT substring_utf8('hello, привет', number + 1, number) FROM numbers(16);
SELECT substring_utf8('hello, привет', -1 - number, 5) FROM numbers(16);
SELECT substring_utf8('hello, привет', -1 - number) FROM numbers(16);
SELECT substring_utf8('hello, привет', 1 + number) FROM numbers(16);

SELECT substring_utf8('hello, привет', 1) FROM numbers(3);
SELECT substring_utf8('hello, привет', 5) FROM numbers(3);
SELECT substring_utf8('hello, привет', 1, 10) FROM numbers(3);
SELECT substring_utf8('hello, привет', 5, 5) FROM numbers(3);
SELECT substring_utf8('hello, привет', -5) FROM numbers(3);
SELECT substring_utf8('hello, привет', -10, 5) FROM numbers(3);

SELECT substring_utf8(materialize('hello, привет'), 1, number) FROM numbers(16);
SELECT substring_utf8(materialize('hello, привет'), number + 1, 3) FROM numbers(16);
SELECT substring_utf8(materialize('hello, привет'), number + 1, number) FROM numbers(16);
SELECT substring_utf8(materialize('hello, привет'), -1 - number, 5) FROM numbers(16);
SELECT substring_utf8(materialize('hello, привет'), -1 - number) FROM numbers(16);
SELECT substring_utf8(materialize('hello, привет'), 1 + number) FROM numbers(16);

SELECT substring_utf8(materialize('hello, привет'), 1) FROM numbers(3);
SELECT substring_utf8(materialize('hello, привет'), 5) FROM numbers(3);
SELECT substring_utf8(materialize('hello, привет'), 1, 10) FROM numbers(3);
SELECT substring_utf8(materialize('hello, привет'), 5, 5) FROM numbers(3);
SELECT substring_utf8(materialize('hello, привет'), -5) FROM numbers(3);
SELECT substring_utf8(materialize('hello, привет'), -10, 5) FROM numbers(3);

SELECT DISTINCT substring(to_string(range(rand(1) % 50)), rand(2) % 50, rand(3) % 50) = substring_utf8(to_string(range(rand(1) % 50)), rand(2) % 50, rand(3) % 50) AS res FROM numbers(1000000);
SELECT DISTINCT substring(to_string(range(rand(1) % 50)), rand(2) % 50) = substring_utf8(to_string(range(rand(1) % 50)), rand(2) % 50) AS res FROM numbers(1000000);

-- NOTE: The behaviour of substring and substring_utf8 is inconsistent when negative offset is greater than string size:
-- substring:
--      hello
-- ^-----^ - offset -10, length 7, result: "he"
-- substring_utf8:
--      hello
--      ^-----^ - offset -10, length 7, result: "hello"

-- SELECT DISTINCT substring(to_string(range(rand(1) % 50)), -(rand(2) % 50), rand(3) % 50) = substring_utf8(to_string(range(rand(1) % 50)), -(rand(2) % 50), rand(3) % 50) AS res FROM numbers(1000000);

SELECT DISTINCT substring(to_string(range(rand(1) % 50)), -(rand(2) % 50)) = substring_utf8(to_string(range(rand(1) % 50)), -(rand(2) % 50)) AS res FROM numbers(1000000);
