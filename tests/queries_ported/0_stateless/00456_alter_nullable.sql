SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS nullable_alter;
create stream nullable_alter (d date DEFAULT '2000-01-01', x string) ENGINE = MergeTree(d, d, 1);

INSERT INTO nullable_alter (x) VALUES ('Hello'), ('World');
SELECT sleep(3);
SELECT x FROM nullable_alter ORDER BY x;

ALTER STREAM nullable_alter MODIFY COLUMN x nullable(string);
SELECT x FROM nullable_alter ORDER BY x;

INSERT INTO nullable_alter (x) VALUES ('xyz'), (NULL);
SELECT sleep(3);
SELECT x FROM nullable_alter ORDER BY x NULLS FIRST;

ALTER STREAM nullable_alter MODIFY COLUMN x nullable(fixed_string(5));
SELECT x FROM nullable_alter ORDER BY x NULLS FIRST;

DROP STREAM nullable_alter;
