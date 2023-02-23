DROP STREAM IF EXISTS userid_set;
DROP STREAM IF EXISTS userid_test;
DROP STREAM IF EXISTS userid_set2;

CREATE STREAM userid_set(userid uint64, name string) ENGINE = Set;
INSERT INTO userid_set VALUES (1, 'Mary'),(2, 'Jane'),(3, 'Mary'),(4, 'Jack');

CREATE STREAM userid_test (userid uint64, name string) ENGINE = MergeTree() PARTITION BY (int_div(userid, 500)) ORDER BY (userid) SETTINGS index_granularity = 8192;
INSERT INTO userid_test VALUES (1, 'Jack'),(2, 'Mary'),(3, 'Mary'),(4, 'John'),(5, 'Mary');

SELECT * FROM userid_test WHERE (userid, name) IN (userid_set);

CREATE STREAM userid_set2(userid uint64, name string, birthdate Date) ENGINE = Set;
INSERT INTO userid_set2 values (1,'John', '1990-01-01');

WITH  'John' AS name,  to_date('1990-01-01') AS birthdate
SELECT * FROM numbers(10)
WHERE (number, name, birthdate) IN (userid_set2);

DROP STREAM userid_set;
DROP STREAM userid_test;
DROP STREAM userid_set2;
