DROP STREAM IF EXISTS table_with_compact_parts;

create stream table_with_compact_parts
(
  date date,
  key uint64,
  value1 string,
  value2 string,
  value3 string
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key
settings index_granularity = 8,
min_rows_for_wide_part = 10;

INSERT INTO table_with_compact_parts SELECT to_date('2019-10-01') + number % 3, number, to_string(number), to_string(number), to_string(number) from numbers(9);

SELECT value1 FROM table_with_compact_parts WHERE key = 1;

ALTER STREAM table_with_compact_parts RENAME COLUMN value1 to renamed_value1;

SELECT renamed_value1 FROM table_with_compact_parts WHERE key = 1;

SELECT * FROM table_with_compact_parts WHERE key = 1 FORMAT TSVWithNames;

ALTER STREAM table_with_compact_parts RENAME COLUMN value2 TO renamed_value2, RENAME COLUMN value3 TO renamed_value3;

SELECT renamed_value2, renamed_value3 FROM table_with_compact_parts WHERE key = 7;

SELECT * FROM table_with_compact_parts WHERE key = 7 FORMAT TSVWithNames;

DROP STREAM IF EXISTS table_with_compact_parts;
