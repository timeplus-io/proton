-- Tags: zookeeper

DROP STREAM IF EXISTS table_rename_with_ttl;

create stream table_rename_with_ttl
(
  date1 date,
  value1 string
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/test/table_rename_with_ttl_01378', '1')
ORDER BY tuple();

INSERT INTO table_rename_with_ttl SELECT to_date('2018-10-01') + number % 3, to_string(number) from numbers(9);

SELECT count() FROM table_rename_with_ttl;

SET materialize_ttl_after_modify = 0;
ALTER STREAM table_rename_with_ttl MODIFY TTL date1 + INTERVAL 1 MONTH;

SELECT count() FROM table_rename_with_ttl;

ALTER STREAM table_rename_with_ttl RENAME COLUMN date1 TO renamed_date1;

ALTER STREAM table_rename_with_ttl materialize TTL settings mutations_sync=2;

SELECT count() FROM table_rename_with_ttl;

DROP STREAM IF EXISTS table_rename_with_ttl;
