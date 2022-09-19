DROP STREAM IF EXISTS multidimensional;
create stream multidimensional (x uint64, arr array(array(string))) ENGINE = MergeTree ORDER BY x;

INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []]);
SELECT * FROM multidimensional;

ALTER STREAM multidimensional ADD COLUMN t tuple(string, array(Nullable(string)), tuple(uint32, date));
INSERT INTO multidimensional (t) VALUES (('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;

OPTIMIZE STREAM multidimensional;
SELECT * FROM multidimensional ORDER BY t;

DROP STREAM multidimensional;

create stream multidimensional (x uint64, arr array(array(string)), t tuple(string, array(Nullable(string)), tuple(uint32, date))) ;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP STREAM multidimensional;

create stream multidimensional (x uint64, arr array(array(string)), t tuple(string, array(Nullable(string)), tuple(uint32, date))) ;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP STREAM multidimensional;

create stream multidimensional (x uint64, arr array(array(string)), t tuple(string, array(Nullable(string)), tuple(uint32, date))) ENGINE = StripeLog;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP STREAM multidimensional;

create stream multidimensional (x uint64, arr array(array(string)), t tuple(string, array(Nullable(string)), tuple(uint32, date)))  ;
INSERT INTO multidimensional VALUES (1, [['Hello', 'World'], ['Goodbye'], []], ('Hello', ['World', NULL], (123, '2000-01-01')));
SELECT * FROM multidimensional ORDER BY t;
DROP STREAM multidimensional;
