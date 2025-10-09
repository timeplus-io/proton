SET query_mode='table';
DROP STREAM IF EXISTS stream_with_compact_parts;

CREATE STREAM stream_with_compact_parts
(
  date Date,
  key uint64,
  value1 string,
  value2 string,
  value3 string
)
PARTITION BY date
ORDER BY key
settings index_granularity = 8,
min_rows_for_wide_part = 10;

select sleep(2) FORMAT Null;

INSERT INTO stream_with_compact_parts(date, key, value1, value2, value3) SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number), to_string(number) from numbers(9);
select sleep(2) FORMAT Null;

SELECT value1 FROM stream_with_compact_parts WHERE key = 1;

ALTER STREAM stream_with_compact_parts RENAME COLUMN value1 to renamed_value1;
select sleep(2) FORMAT Null;

SELECT renamed_value1 FROM stream_with_compact_parts WHERE key = 1;

SELECT (* except _tp_time) FROM stream_with_compact_parts WHERE key = 1 FORMAT TSVWithNames;

ALTER STREAM stream_with_compact_parts RENAME COLUMN value2 TO renamed_value2;
select sleep(2) FORMAT Null;

ALTER STREAM stream_with_compact_parts RENAME COLUMN value3 TO renamed_value3;
select sleep(2) FORMAT Null;

SELECT renamed_value2, renamed_value3 FROM stream_with_compact_parts WHERE key = 7;

SELECT (* except _tp_time) FROM stream_with_compact_parts WHERE key = 7 FORMAT TSVWithNames;

DROP STREAM IF EXISTS stream_with_compact_parts;
