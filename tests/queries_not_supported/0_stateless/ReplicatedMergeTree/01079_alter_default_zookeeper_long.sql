-- Tags: long, zookeeper

DROP STREAM IF EXISTS alter_default;

create stream alter_default
(
  date date,
  key uint64
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01079/alter_default', '1')
ORDER BY key;

INSERT INTO alter_default select to_date('2020-01-05'), number from system.numbers limit 100;

-- Cannot add column without type
ALTER STREAM alter_default ADD COLUMN value DEFAULT '10'; --{serverError 36}

ALTER STREAM alter_default ADD COLUMN value string DEFAULT '10';

SHOW create stream alter_default;

SELECT sum(cast(value as uint64)) FROM alter_default;

ALTER STREAM alter_default MODIFY COLUMN value uint64;

SHOW create stream alter_default;

ALTER STREAM alter_default MODIFY COLUMN value uint64 DEFAULT 10;

SHOW create stream alter_default;

SELECT sum(value) from alter_default;

ALTER STREAM alter_default MODIFY COLUMN value DEFAULT 100;

SHOW create stream alter_default;

ALTER STREAM alter_default MODIFY COLUMN value uint16 DEFAULT 100;

SHOW create stream alter_default;

SELECT sum(value) from alter_default;

ALTER STREAM alter_default MODIFY COLUMN value uint8 DEFAULT 10;

SHOW create stream alter_default;

ALTER STREAM alter_default ADD COLUMN bad_column uint8 DEFAULT 'q'; --{serverError 6}

ALTER STREAM alter_default ADD COLUMN better_column uint8 DEFAULT '1';

SHOW create stream alter_default;

ALTER STREAM alter_default ADD COLUMN other_date string DEFAULT '0';

ALTER STREAM alter_default MODIFY COLUMN other_date datetime; --{serverError 41}

ALTER STREAM alter_default MODIFY COLUMN other_date DEFAULT 1;

SHOW create stream alter_default;

DROP STREAM IF EXISTS alter_default;
