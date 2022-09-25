-- Tags: long, zookeeper

DROP STREAM IF EXISTS table_rename_with_default;

create stream table_rename_with_default
(
  date date,
  key uint64,
  value1 string,
  value2 string DEFAULT concat('Hello ', value1),
  value3 string ALIAS concat('Word ', value1)
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key;

INSERT INTO table_rename_with_default (date, key, value1) SELECT to_datetime(to_date('2019-10-01') + number % 3, 'Europe/Moscow'), number, to_string(number)  from numbers(9);

SELECT * FROM table_rename_with_default WHERE key = 1 FORMAT TSVWithNames;

SHOW create stream table_rename_with_default;

ALTER STREAM table_rename_with_default RENAME COLUMN value1 TO renamed_value1;

SELECT * FROM table_rename_with_default WHERE key = 1 FORMAT TSVWithNames;

SHOW create stream table_rename_with_default;

SELECT value2 FROM table_rename_with_default WHERE key = 1;
SELECT value3 FROM table_rename_with_default WHERE key = 1;

DROP STREAM IF EXISTS table_rename_with_default;

DROP STREAM IF EXISTS table_rename_with_ttl;

create stream table_rename_with_ttl
(
  date1 date,
  date2 date,
  value1 string,
  value2 string TTL date1 + INTERVAL 500 MONTH
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test_01213/table_rename_with_ttl', '1')
ORDER BY tuple()
TTL date2 + INTERVAL 500 MONTH;

INSERT INTO table_rename_with_ttl SELECT to_datetime(to_date('2019-10-01') + number % 3, 'Europe/Moscow'), to_datetime(to_date('2018-10-01') + number % 3, 'Europe/Moscow'), to_string(number), to_string(number) from numbers(9);

SELECT * FROM table_rename_with_ttl WHERE value1 = '1' FORMAT TSVWithNames;

SHOW create stream table_rename_with_ttl;

ALTER STREAM table_rename_with_ttl RENAME COLUMN date1 TO renamed_date1;

SELECT * FROM table_rename_with_ttl WHERE value1 = '1' FORMAT TSVWithNames;

SHOW create stream table_rename_with_ttl;

ALTER STREAM table_rename_with_ttl RENAME COLUMN date2 TO renamed_date2;

SELECT * FROM table_rename_with_ttl WHERE value1 = '1' FORMAT TSVWithNames;

SHOW create stream table_rename_with_ttl;

DROP STREAM IF EXISTS table_rename_with_ttl;
