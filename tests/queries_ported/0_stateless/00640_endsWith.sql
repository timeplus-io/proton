SELECT ends_with(s, 'ow') FROM (SELECT array_join(['', 'o', 'ow', 'Hellow', '3434', 'owfffffffdHe']) AS s);
SELECT ends_with(s, '') FROM (SELECT array_join(['', 'h', 'hi']) AS s);
SELECT ends_with('123', '3');
SELECT ends_with('123', '23');
SELECT ends_with('123', '32');
SELECT ends_with('123', '');

DROP STREAM IF EXISTS endsWith_test;
create stream endsWith_test(S1 string, S2 string, S3 fixed_string(2)) ENGINE=Memory;
INSERT INTO endsWith_test values ('11', '22', '33'), ('a', 'a', 'bb'), ('abc', 'bc', '23');

SELECT count() FROM endsWith_test WHERE ends_with(S1, S1);
SELECT count() FROM endsWith_test WHERE ends_with(S1, S2);
SELECT count() FROM endsWith_test WHERE ends_with(S2, S3);

SELECT ends_with([], 'str'); -- { serverError 43 }
DROP STREAM endsWith_test;
