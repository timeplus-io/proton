SET query_mode='table';
DROP STREAM IF EXISTS stream_for_rename;

CREATE STREAM stream_for_rename
(
  date date,
  key uint64,
  value1 string,
  value2 string,
  value3 string
);

select sleep(2) FORMAT Null;

INSERT INTO stream_for_rename(date, key, value1, value2, value3) SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number), to_string(number) from numbers(9);
select sleep(2) FORMAT Null;

SELECT value1 FROM stream_for_rename WHERE key = 1;

ALTER STREAM stream_for_rename RENAME COLUMN value1 to renamed_value1;
select sleep(2) FORMAT Null;

SELECT renamed_value1 FROM stream_for_rename WHERE key = 1;

SELECT (* except _tp_time) FROM stream_for_rename WHERE key = 1 FORMAT TSVWithNames;

ALTER STREAM stream_for_rename RENAME COLUMN value3 to value2; --{serverError 15}
ALTER STREAM stream_for_rename RENAME COLUMN value3 TO r1, RENAME COLUMN value3 TO r2; --{serverError 36}
ALTER STREAM stream_for_rename RENAME COLUMN value3 TO r1, RENAME COLUMN r1 TO value1; --{serverError 48}

ALTER STREAM stream_for_rename RENAME COLUMN value2 TO renamed_value2;
select sleep(2) FORMAT Null;
ALTER STREAM stream_for_rename RENAME COLUMN value3 TO renamed_value3;
select sleep(2) FORMAT Null;

SELECT renamed_value2, renamed_value3 FROM stream_for_rename WHERE key = 7;

SELECT (* except _tp_time) FROM stream_for_rename WHERE key = 7 FORMAT TSVWithNames;

ALTER STREAM stream_for_rename RENAME COLUMN value100 to renamed_value100; --{serverError 10}
ALTER STREAM stream_for_rename RENAME COLUMN IF EXISTS value100 to renamed_value100;
select sleep(2) FORMAT Null;

DROP STREAM IF EXISTS stream_for_rename;
