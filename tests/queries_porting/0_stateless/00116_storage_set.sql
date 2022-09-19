-- Tags: no-parallel
SET query_mode='table';
DROP STREAM IF EXISTS set;
DROP STREAM IF EXISTS set2;

create stream set (x string) ENGINE = Set;

SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s NOT IN set;

INSERT INTO set VALUES ('Hello'), ('World');
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

RENAME TABLE set TO set2;
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO set2 VALUES ('Hello'), ('World');
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

INSERT INTO set2 VALUES ('abc'), ('World');
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

DETACH TABLE set2;
ATTACH TABLE set2;

SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set2;

RENAME TABLE set2 TO set;
SELECT array_join(['Hello', 'test', 'World', 'world', 'abc', 'xyz']) AS s WHERE s IN set;

DROP STREAM set;
