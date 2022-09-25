SELECT startsWith(s, 'He') FROM (SELECT array_join(['', 'H', 'He', 'Hellow', '3434', 'fffffffffdHe']) AS s);
SELECT startsWith(s, '') FROM (SELECT array_join(['', 'h', 'hi']) AS s);
SELECT startsWith('123', '123');
SELECT startsWith('123', '12');
SELECT startsWith('123', '1234');
SELECT startsWith('123', '');

DROP STREAM IF EXISTS startsWith_test;
create stream startsWith_test(S1 string, S2 string, S3 fixed_string(2)) ENGINE=Memory;
INSERT INTO startsWith_test values ('11', '22', '33'), ('a', 'a', 'bb'), ('abc', 'ab', '23');

SELECT count() FROM startsWith_test WHERE startsWith(S1, S1);
SELECT count() FROM startsWith_test WHERE startsWith(S1, S2);
SELECT count() FROM startsWith_test WHERE startsWith(S2, S3);
DROP STREAM startsWith_test;
