SELECT endsWith(s, 'ow') FROM (SELECT array_join(['', 'o', 'ow', 'Hellow', '3434', 'owfffffffdHe']) AS s);
SELECT endsWith(s, '') FROM (SELECT array_join(['', 'h', 'hi']) AS s);
SELECT endsWith('123', '3');
SELECT endsWith('123', '23');
SELECT endsWith('123', '32');
SELECT endsWith('123', '');

DROP STREAM IF EXISTS endsWith_test;
create stream endsWith_test(S1 string, S2 string, S3 fixed_string(2)) ENGINE=Memory;
INSERT INTO endsWith_test values ('11', '22', '33'), ('a', 'a', 'bb'), ('abc', 'bc', '23');

SELECT count() FROM endsWith_test WHERE endsWith(S1, S1);
SELECT count() FROM endsWith_test WHERE endsWith(S1, S2);
SELECT count() FROM endsWith_test WHERE endsWith(S2, S3);

SELECT endsWith([], 'str'); -- { serverError 43 }
DROP STREAM endsWith_test;
