DROP STREAM IF EXISTS add_table;

create stream add_table
(
    key uint64,
    value1 string
)
ENGINE = MergeTree()
ORDER BY key;

SHOW create stream add_table;

ALTER STREAM add_table ADD COLUMN IF NOT EXISTS value1 uint64;

SHOW create stream add_table;

ALTER STREAM add_table ADD COLUMN IF NOT EXISTS key string, ADD COLUMN IF NOT EXISTS value1 uint64;

SHOW create stream add_table;

ALTER STREAM add_table ADD COLUMN IF NOT EXISTS value1 uint64, ADD COLUMN IF NOT EXISTS value2 uint64;

SHOW create stream add_table;

ALTER STREAM add_table ADD COLUMN value3 uint64, ADD COLUMN IF NOT EXISTS value3 uint32; --{serverError 44}

DROP STREAM IF EXISTS add_table;
