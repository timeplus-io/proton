DROP STREAM IF EXISTS default_table;

create stream default_table (
  key uint64 DEFAULT 42,
  value1 uint64 MATERIALIZED key * key,
  value2 ALIAS value1 * key
)
ENGINE = MergeTree()
ORDER BY tuple();

ALTER STREAM default_table MODIFY COLUMN key REMOVE MATERIALIZED; --{serverError 36}
ALTER STREAM default_table MODIFY COLUMN key REMOVE ALIAS; --{serverError 36}

ALTER STREAM default_table MODIFY COLUMN value1 REMOVE DEFAULT; --{serverError 36}
ALTER STREAM default_table MODIFY COLUMN value1 REMOVE ALIAS; --{serverError 36}

ALTER STREAM default_table MODIFY COLUMN value2 REMOVE DEFAULT; --{serverError 36}
ALTER STREAM default_table MODIFY COLUMN value2 REMOVE MATERIALIZED; --{serverError 36}

SHOW create stream default_table;

DROP STREAM IF EXISTS default_table;
