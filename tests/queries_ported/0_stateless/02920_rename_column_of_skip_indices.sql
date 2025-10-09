DROP STREAM IF EXISTS t;

CREATE STREAM t
(
  key1 uint64,
  value1 string,
  value2 string,
  INDEX idx (value1) TYPE set(10) GRANULARITY 1
)
ENGINE MergeTree ORDER BY key1 SETTINGS index_granularity = 1;

INSERT INTO t SELECT to_Date('2019-10-01') + number % 3, to_String(number), to_String(number) from numbers(9);

SYSTEM STOP MERGES t;

SET alter_sync = 0;

ALTER STREAM t RENAME COLUMN value1 TO value11;

-- Index works without mutation applied.
SELECT * FROM t WHERE value11 = '000' SETTINGS max_rows_to_read = 0;

SYSTEM START MERGES t;

-- Another ALTER to wait for.
ALTER STREAM t RENAME COLUMN value11 TO value12 SETTINGS mutations_sync = 2;

-- Index works with mutation applied.
SELECT * FROM t WHERE value12 = '000' SETTINGS max_rows_to_read = 0;

DROP STREAM t;
