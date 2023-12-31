DROP STREAM IF EXISTS stream_for_rename;

CREATE STREAM stream_for_rename
(
  date Date,
  key uint64,
  value1 string,
  value2 string,
  value3 string
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key;

INSERT INTO stream_for_rename SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number), to_string(number) from numbers(9);

SELECT value1 FROM stream_for_rename WHERE key = 1;

ALTER STREAM stream_for_rename RENAME COLUMN value1 to renamed_value1;

SELECT renamed_value1 FROM stream_for_rename WHERE key = 1;

SELECT * FROM stream_for_rename WHERE key = 1 FORMAT TSVWithNames;

ALTER STREAM stream_for_rename RENAME COLUMN value3 to value2; --{serverError 15}
ALTER STREAM stream_for_rename RENAME COLUMN value3 TO r1, RENAME COLUMN value3 TO r2; --{serverError 36}
ALTER STREAM stream_for_rename RENAME COLUMN value3 TO r1, RENAME COLUMN r1 TO value1; --{serverError 48}

ALTER STREAM stream_for_rename RENAME COLUMN value2 TO renamed_value2, RENAME COLUMN value3 TO renamed_value3;

SELECT renamed_value2, renamed_value3 FROM stream_for_rename WHERE key = 7;

SELECT * FROM stream_for_rename WHERE key = 7 FORMAT TSVWithNames;

ALTER STREAM stream_for_rename RENAME COLUMN value100 to renamed_value100; --{serverError 10}
ALTER STREAM stream_for_rename RENAME COLUMN IF EXISTS value100 to renamed_value100;

DROP STREAM IF EXISTS stream_for_rename;
