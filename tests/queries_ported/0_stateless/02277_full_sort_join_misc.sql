SET join_algorithm = 'full_sorting_merge';

SELECT * FROM (SELECT 1 as key) AS t1 JOIN (SELECT 1 as key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT * FROM (SELECT 1 as key) AS t1 JOIN (SELECT 1 as key) AS t2 USING key ORDER BY key;

SELECT * FROM (SELECT 1 :: uint32 as key) AS t1 FULL JOIN (SELECT 1 :: nullable(uint32) as key) AS t2 USING (key) ORDER BY key;

SELECT * FROM (SELECT 1 :: uint32 as key) AS t1 FULL JOIN (SELECT NULL :: nullable(uint32) as key) AS t2 USING (key) ORDER BY key;

SELECT * FROM (SELECT 1 :: int32 as key) AS t1 JOIN (SELECT 1 :: uint32 as key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT * FROM (SELECT -1 :: nullable(int32) as key) AS t1 FULL JOIN (SELECT 4294967295 :: uint32 as key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT * FROM (SELECT 'a' :: low_cardinality(string) AS key) AS t1 JOIN (SELECT 'a' :: string AS key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT * FROM (SELECT 'a' :: low_cardinality(nullable(string)) AS key) AS t1 JOIN (SELECT 'a' :: string AS key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT * FROM (SELECT 'a' :: low_cardinality(nullable(string)) AS key) AS t1 JOIN (SELECT 'a' :: nullable(string) AS key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT * FROM (SELECT 'a' :: low_cardinality(string) AS key) AS t1 JOIN (SELECT 'a' :: low_cardinality(string) AS key) AS t2 ON t1.key = t2.key ORDER BY key;

SELECT 5 == count() FROM (SELECT number as a from numbers(5)) as t1 LEFT JOIN (SELECT number as b from numbers(5) WHERE number > 100) as t2 ON t1.a = t2.b ORDER BY 1;
SELECT 5 == count() FROM (SELECT number as a from numbers(5) WHERE number > 100) as t1 RIGHT JOIN (SELECT number as b from numbers(5)) as t2 ON t1.a = t2.b ORDER BY 1;
