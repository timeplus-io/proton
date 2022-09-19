DROP STREAM IF EXISTS constraint_test_assumption;
DROP STREAM IF EXISTS constraint_test_transitivity;
DROP STREAM IF EXISTS constraint_test_transitivity2;

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_substitute_columns = 1;
SET optimize_append_index = 1;

create stream constraint_test_assumption (URL string, a int32, CONSTRAINT c1 ASSUME domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT c2 ASSUME URL > 'zzz' AND startsWith(URL, 'test') = True) ;

--- Add wrong rows in order to check optimization
INSERT INTO constraint_test_assumption (URL, a) VALUES ('1', 1);
INSERT INTO constraint_test_assumption (URL, a) VALUES ('2', 2);
INSERT INTO constraint_test_assumption (URL, a) VALUES ('yandex.ru', 3);
INSERT INTO constraint_test_assumption (URL, a) VALUES ('3', 4);

SELECT count() FROM constraint_test_assumption WHERE domainWithoutWWW(URL) = 'yandex.ru'; --- assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE NOT (domainWithoutWWW(URL) = 'yandex.ru'); --- assumption -> 0
SELECT count() FROM constraint_test_assumption WHERE domainWithoutWWW(URL) != 'yandex.ru'; --- assumption -> 0
SELECT count() FROM constraint_test_assumption WHERE domainWithoutWWW(URL) = 'nothing'; --- not optimized -> 0

SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL > 'zzz'); ---> assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND NOT URL <= 'zzz'); ---> assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL > 'zzz') OR (a = 10 AND a + 5 < 100); ---> assumption -> 4
SELECT count() FROM constraint_test_assumption WHERE (domainWithoutWWW(URL) = 'yandex.ru' AND URL = '111'); ---> assumption & no assumption -> 0
SELECT count() FROM constraint_test_assumption WHERE (startsWith(URL, 'test') = True); ---> assumption -> 4

DROP STREAM constraint_test_assumption;

create stream constraint_test_transitivity (a int64, b int64, c int64, d int32, CONSTRAINT c1 ASSUME a = b AND c = d, CONSTRAINT c2 ASSUME b = c) ;

INSERT INTO constraint_test_transitivity (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test_transitivity WHERE a = d; ---> assumption -> 1

DROP STREAM constraint_test_transitivity;


create stream constraint_test_strong_connectivity (a string, b string, c string, d string, CONSTRAINT c1 ASSUME a <= b AND b <= c AND c <= d AND d <= a) ;

INSERT INTO constraint_test_strong_connectivity (a, b, c, d) VALUES ('1', '2', '3', '4');

SELECT count() FROM constraint_test_strong_connectivity WHERE a = d; ---> assumption -> 1
SELECT count() FROM constraint_test_strong_connectivity WHERE a = c AND b = d; ---> assumption -> 1
SELECT count() FROM constraint_test_strong_connectivity WHERE a < c OR b < d; ---> assumption -> 0
SELECT count() FROM constraint_test_strong_connectivity WHERE a <= c OR b <= d; ---> assumption -> 1

DROP STREAM constraint_test_strong_connectivity;

create stream constraint_test_transitivity2 (a string, b string, c string, d string, CONSTRAINT c1 ASSUME a > b AND b >= c AND c > d AND a >= d) ;

INSERT INTO constraint_test_transitivity2 (a, b, c, d) VALUES ('1', '2', '3', '4');

SELECT count() FROM constraint_test_transitivity2 WHERE a > d; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE a >= d; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE d < a; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity2 WHERE a < d; ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity2 WHERE a = d; ---> assumption -> 0
SELECT count() FROM constraint_test_transitivity2 WHERE a != d; ---> assumption -> 1

DROP STREAM constraint_test_transitivity2;

create stream constraint_test_transitivity3 (a int64, b int64, c int64, CONSTRAINT c1 ASSUME b > 10 AND 1 > a) ;

INSERT INTO constraint_test_transitivity3 (a, b, c) VALUES (4, 0, 2);

SELECT count() FROM constraint_test_transitivity3 WHERE a < b; ---> assumption -> 1
SELECT count() FROM constraint_test_transitivity3 WHERE b >= a; ---> assumption -> 1

DROP STREAM constraint_test_transitivity3;


create stream constraint_test_constants_repl (a int64, b int64, c int64, d int64, CONSTRAINT c1 ASSUME a - b = 10 AND c + d = 20) ;

INSERT INTO constraint_test_constants_repl (a, b, c, d) VALUES (1, 2, 3, 4);

SELECT count() FROM constraint_test_constants_repl WHERE a - b = 10; ---> assumption -> 1
SELECT count() FROM constraint_test_constants_repl WHERE a - b < 0; ---> assumption -> 0
SELECT count() FROM constraint_test_constants_repl WHERE a - b = c + d; ---> assumption -> 0
SELECT count() FROM constraint_test_constants_repl WHERE (a - b) * 2 = c + d; ---> assumption -> 1

DROP STREAM constraint_test_constants_repl;

create stream constraint_test_constants (a int64, b int64, c int64, CONSTRAINT c1 ASSUME b > 10 AND a >= 10) ;

INSERT INTO constraint_test_constants (a, b, c) VALUES (0, 0, 0);

SELECT count() FROM constraint_test_constants WHERE 9 < b; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 11 < b; ---> assumption -> 0
SELECT count() FROM constraint_test_constants WHERE 10 <= b; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 9 < a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 10 < a; ---> assumption -> 0
SELECT count() FROM constraint_test_constants WHERE 10 <= a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 9 <= a; ---> assumption -> 1
SELECT count() FROM constraint_test_constants WHERE 11 <= a; ---> assumption -> 0

-- A AND NOT A
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100);
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100);
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c > 100);
EXPLAIN SYNTAX SELECT count() FROM constraint_test_constants WHERE (a > 100 OR b > 100 OR c > 100) AND (a <= 100 OR b > 100 OR c > 100) AND (NOT b > 100 OR c > 100) AND (c <= 100);

DROP STREAM constraint_test_constants;
