-- Tags: long, zookeeper

DROP STREAM IF EXISTS table_for_rename1;

create stream table_for_rename1
(
  date date,
  key uint64,
  value1 string,
  value2 string,
  value3 string,
  CONSTRAINT cs_value1 CHECK to_int64(value1) < to_int64(value2),
  CONSTRAINT cs_value2 CHECK to_int64(value2) < to_int64(value3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01277/test_for_rename', '1')
PARTITION BY date
ORDER BY key;

INSERT INTO table_for_rename1 SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number + 1), to_string(number + 2) from numbers(9);
INSERT INTO table_for_rename1 SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number + 1), to_string(number) from numbers(9); ; --{serverError 469}

SELECT * FROM table_for_rename1 ORDER BY key;

ALTER STREAM table_for_rename1 RENAME COLUMN value1 TO value4;
ALTER STREAM table_for_rename1 RENAME COLUMN value2 TO value5;
SHOW create stream table_for_rename1;
SELECT * FROM table_for_rename1 ORDER BY key;

SELECT '-- insert after rename --';
INSERT INTO table_for_rename1 SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number + 1), to_string(number + 2) from numbers(10, 10);
INSERT INTO table_for_rename1 SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number + 1), to_string(number) from numbers(10, 10); ; --{serverError 469}
SELECT * FROM table_for_rename1 ORDER BY key;

SELECT '-- rename columns back --';
ALTER STREAM table_for_rename1 RENAME COLUMN value4 TO value1;
ALTER STREAM table_for_rename1 RENAME COLUMN value5 TO value2;
SHOW create stream table_for_rename1;
SELECT * FROM table_for_rename1 ORDER BY key;

SELECT '-- insert after rename column --';
INSERT INTO table_for_rename1 SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number + 1), to_string(number + 2) from numbers(20,10);
INSERT INTO table_for_rename1 SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number), to_string(number + 2) from numbers(20, 10); ; --{serverError 469}
SELECT * FROM table_for_rename1 ORDER BY key;

DROP STREAM IF EXISTS table_for_rename1;
