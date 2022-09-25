-- Tags: no-parallel
SET query_mode='table';
DROP STREAM IF EXISTS set;
DROP STREAM IF EXISTS set2;

create stream set (x string) ENGINE = Set;

SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s NOT IN set;

INSERT INTO set(x) VALUES ('Hello'), ('World');
SELECT sleep(3);
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

RENAME STREAM set TO set2;
SELECT sleep(3);
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO set2(x) VALUES ('Hello'), ('World');
SELECT sleep(3);
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO set2 (x) VALUES ('abc'), ('World');
SELECT sleep(3);
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

DETACH STREAM set2;
ATTACH STREAM set2;

SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

RENAME STREAM set2 TO set;
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

DROP STREAM set;
