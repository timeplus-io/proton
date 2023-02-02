WITH
	to_datetime64('2019-09-16 19:20:12.345678910', 3) AS dt64
SELECT
	dt64,
	from_unix_timestamp64_milli(to_unix_timestamp64_milli(dt64)),
	from_unix_timestamp64_micro(to_unix_timestamp64_micro(dt64)),
	from_unix_timestamp64_nano(to_unix_timestamp64_nano(dt64));

WITH
	to_datetime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT
	dt64,
	from_unix_timestamp64_milli(to_unix_timestamp64_milli(dt64)),
	from_unix_timestamp64_micro(to_unix_timestamp64_micro(dt64)),
	from_unix_timestamp64_nano(to_unix_timestamp64_nano(dt64));

WITH
	to_datetime64('2019-09-16 19:20:12.345678910', 9) AS dt64
SELECT
	dt64,
	from_unix_timestamp64_milli(to_unix_timestamp64_milli(dt64)),
	from_unix_timestamp64_micro(to_unix_timestamp64_micro(dt64)),
	from_unix_timestamp64_nano(to_unix_timestamp64_nano(dt64));

SELECT 'with explicit timezone';
WITH
	'UTC' as timezone,
	to_datetime64('2019-09-16 19:20:12.345678910', 3, timezone) AS dt64
SELECT
	dt64,
	from_unix_timestamp64_milli(to_unix_timestamp64_milli(dt64), timezone),
	from_unix_timestamp64_micro(to_unix_timestamp64_micro(dt64), timezone),
	from_unix_timestamp64_nano(to_unix_timestamp64_nano(dt64), timezone) AS v,
	to_type_name(v);

WITH
	'Asia/Makassar' as timezone,
	to_datetime64('2019-09-16 19:20:12.345678910', 3, timezone) AS dt64
SELECT
	dt64,
	from_unix_timestamp64_milli(to_unix_timestamp64_milli(dt64), timezone),
	from_unix_timestamp64_micro(to_unix_timestamp64_micro(dt64), timezone),
	from_unix_timestamp64_nano(to_unix_timestamp64_nano(dt64), timezone) AS v,
	to_type_name(v);


WITH
	CAST(1234567891011 AS int64) AS val
SELECT
	val,
	to_unix_timestamp64_milli(from_unix_timestamp64_milli(val)),
	to_unix_timestamp64_micro(from_unix_timestamp64_micro(val)),
	to_unix_timestamp64_nano(from_unix_timestamp64_nano(val));

SELECT 'with explicit timezone';
WITH
	'UTC' as timezone,
	CAST(1234567891011 AS int64) AS val
SELECT
	val,
	to_unix_timestamp64_milli(from_unix_timestamp64_milli(val, timezone)),
	to_unix_timestamp64_micro(from_unix_timestamp64_micro(val, timezone)),
	to_unix_timestamp64_nano(from_unix_timestamp64_nano(val, timezone)) AS v,
	to_type_name(v);