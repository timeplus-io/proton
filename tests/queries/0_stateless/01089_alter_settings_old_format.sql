DROP STREAM IF EXISTS old_format_mt;

create stream old_format_mt (
  event_date date,
  key uint64,
  value1 uint64,
  value2 string
)
ENGINE = MergeTree(event_date, (key, value1), 8192);

ALTER STREAM old_format_mt MODIFY SETTING enable_mixed_granularity_parts = 1; --{serverError 36}

SELECT 1;

DROP STREAM IF EXISTS old_format_mt;
