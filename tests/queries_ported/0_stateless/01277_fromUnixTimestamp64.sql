-- -- Error cases
SELECT from_unix_timestamp64_milli();  -- {serverError 42}
SELECT from_unix_timestamp64_micro();  -- {serverError 42}
SELECT from_unix_timestamp64_nano();  -- {serverError 42}

SELECT from_unix_timestamp64_milli('abc');  -- {serverError 43}
SELECT from_unix_timestamp64_micro('abc');  -- {serverError 43}
SELECT from_unix_timestamp64_nano('abc');  -- {serverError 43}

SELECT from_unix_timestamp64_milli('abc', 123);  -- {serverError 43}
SELECT from_unix_timestamp64_micro('abc', 123);  -- {serverError 43}
SELECT from_unix_timestamp64_nano('abc', 123);  -- {serverError 43}

SELECT 'const column';
WITH
	CAST(1234567891011 AS int64) AS i64,
	'UTC' AS tz
SELECT
	tz,
	i64,
	from_unix_timestamp64_milli(i64, tz),
	from_unix_timestamp64_micro(i64, tz),
	from_unix_timestamp64_nano(i64, tz) as dt64,
	to_type_name(dt64);

WITH
	CAST(1234567891011 AS int64) AS i64,
	'Asia/Makassar' AS tz
SELECT
	tz,
	i64,
	from_unix_timestamp64_milli(i64, tz),
	from_unix_timestamp64_micro(i64, tz),
	from_unix_timestamp64_nano(i64, tz) as dt64,
	to_type_name(dt64);

SELECT 'non-const column';
WITH
	CAST(1234567891011 AS int64) AS i64,
	'UTC' AS tz
SELECT
	i64,
	from_unix_timestamp64_milli(i64, tz),
	from_unix_timestamp64_micro(i64, tz),
	from_unix_timestamp64_nano(i64, tz) as dt64;

SELECT 'upper range bound';
WITH
    10413688942 AS timestamp,
    CAST(10413688942123 AS int64) AS milli,
    CAST(10413688942123456 AS int64) AS micro,
    CAST(10413688942123456789 AS int64) AS nano,
    'UTC' AS tz
SELECT
    timestamp,
    from_unix_timestamp64_milli(milli, tz),
    from_unix_timestamp64_micro(micro, tz),
    from_unix_timestamp64_nano(nano, tz);

SELECT 'lower range bound';
WITH
    -2208985199 AS timestamp,
    CAST(-2208985199123 AS int64) AS milli,
    CAST(-2208985199123456 AS int64) AS micro,
    CAST(-2208985199123456789 AS int64) AS nano,
    'UTC' AS tz
SELECT
    timestamp,
    from_unix_timestamp64_milli(milli, tz),
    from_unix_timestamp64_micro(micro, tz),
    from_unix_timestamp64_nano(nano, tz);
