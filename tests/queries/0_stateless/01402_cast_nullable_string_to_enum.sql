SET cast_keep_nullable = 0;

-- https://github.com/ClickHouse/ClickHouse/issues/5818#issuecomment-619628445
SELECT CAST(CAST(NULL AS nullable(string)) AS nullable(enum8('Hello' = 1)));
SELECT CAST(CAST(NULL AS nullable(fixed_string(1))) AS nullable(enum8('Hello' = 1)));

-- empty string still not acceptable
SELECT CAST(CAST('' AS nullable(string)) AS nullable(enum8('Hello' = 1))); -- { serverError 36; }
SELECT CAST(CAST('' AS nullable(fixed_string(1))) AS nullable(enum8('Hello' = 1))); -- { serverError 36; }

-- non-nullable Enum() still not acceptable
SELECT CAST(CAST(NULL AS nullable(string)) AS enum8('Hello' = 1)); -- { serverError 349; }
SELECT CAST(CAST(NULL AS nullable(fixed_string(1))) AS enum8('Hello' = 1)); -- { serverError 349; }
