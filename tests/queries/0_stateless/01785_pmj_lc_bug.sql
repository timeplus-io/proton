SET join_algorithm = 'partial_merge';
SET max_bytes_in_join = '100';

create stream foo_lc (n LowCardinality(string)) ;
create stream foo (n string) ;

INSERT INTO foo SELECT to_string(number) AS n FROM system.numbers LIMIT 1025;
INSERT INTO foo_lc SELECT to_string(number) AS n FROM system.numbers LIMIT 1025;

SELECT 1025 == count(n) FROM foo_lc AS t1 ANY LEFT JOIN foo_lc AS t2 ON t1.n == t2.n;
SELECT 1025 == count(n) FROM foo AS t1 ANY LEFT JOIN foo_lc AS t2 ON t1.n == t2.n;
SELECT 1025 == count(n) FROM foo_lc AS t1 ANY LEFT JOIN foo AS t2 ON t1.n == t2.n;

SELECT 1025 == count(n) FROM foo_lc AS t1 ALL LEFT JOIN foo_lc AS t2 ON t1.n == t2.n;

DROP STREAM foo;
DROP STREAM foo_lc;
