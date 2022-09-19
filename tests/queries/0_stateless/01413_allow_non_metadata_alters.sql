DROP STREAM IF EXISTS non_metadata_alters;

create stream non_metadata_alters (
  key uint64,
  value1 string,
  value2 Enum8('Hello' = 1, 'World' = 2),
  value3 uint16,
  value4 DateTime,
  value5 date
)
ENGINE = MergeTree()
ORDER BY tuple();


SET allow_non_metadata_alters = 0;

ALTER STREAM non_metadata_alters MODIFY COLUMN value3 uint64; --{serverError 524}

ALTER STREAM non_metadata_alters MODIFY COLUMN value1 uint32; --{serverError 524}

ALTER STREAM non_metadata_alters MODIFY COLUMN value4 date; --{serverError 524}

ALTER STREAM non_metadata_alters DROP COLUMN value4; --{serverError 524}

ALTER STREAM non_metadata_alters MODIFY COLUMN value2 Enum8('x' = 5, 'y' = 6); --{serverError 524}

ALTER STREAM non_metadata_alters RENAME COLUMN value4 TO renamed_value4; --{serverError 524}

ALTER STREAM non_metadata_alters MODIFY COLUMN value3 uint16 TTL value5 + INTERVAL 5 DAY; --{serverError 524}

SET materialize_ttl_after_modify = 0;

ALTER STREAM non_metadata_alters MODIFY COLUMN value3 uint16 TTL value5 + INTERVAL 5 DAY;

SHOW create stream non_metadata_alters;

ALTER STREAM non_metadata_alters MODIFY COLUMN value1 string DEFAULT 'X';

ALTER STREAM non_metadata_alters MODIFY COLUMN value2 Enum8('Hello' = 1, 'World' = 2, '!' = 3);

ALTER STREAM non_metadata_alters MODIFY COLUMN value3 date;

ALTER STREAM non_metadata_alters MODIFY COLUMN value4 uint32;

ALTER STREAM non_metadata_alters ADD COLUMN value6 Decimal(3, 3);

SHOW create stream non_metadata_alters;

DROP STREAM IF EXISTS non_metadata_alters;
