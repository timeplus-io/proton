-- Error cases
SELECT to_unix_timestamp64_milli();  -- {serverError 42}
SELECT to_unix_timestamp64_micro();  -- {serverError 42}
SELECT to_unix_timestamp64_nano();  -- {serverError 42}

SELECT to_unix_timestamp64_milli('abc');  -- {serverError 43}
SELECT to_unix_timestamp64_micro('abc');  -- {serverError 43}
SELECT to_unix_timestamp64_nano('abc');  -- {serverError 43}

SELECT to_unix_timestamp64_milli('abc', 123);  -- {serverError 42}
SELECT to_unix_timestamp64_micro('abc', 123);  -- {serverError 42}
SELECT to_unix_timestamp64_nano('abc', 123);  -- {serverError 42}

SELECT 'const column';
WITH to_datetime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS dt64
SELECT dt64, to_unix_timestamp64_milli(dt64), to_unix_timestamp64_micro(dt64), to_unix_timestamp64_nano(dt64);

WITH to_datetime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS dt64
SELECT dt64, to_unix_timestamp64_milli(dt64), to_unix_timestamp64_micro(dt64), to_unix_timestamp64_nano(dt64);

WITH to_datetime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS dt64
SELECT dt64, to_unix_timestamp64_milli(dt64), to_unix_timestamp64_micro(dt64), to_unix_timestamp64_nano(dt64);

SELECT 'non-const column';
WITH to_datetime64('2019-09-16 19:20:12.345678910', 3, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, to_unix_timestamp64_milli(dt64), to_unix_timestamp64_micro(dt64), to_unix_timestamp64_nano(dt64);

WITH to_datetime64('2019-09-16 19:20:12.345678910', 6, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, to_unix_timestamp64_milli(dt64), to_unix_timestamp64_micro(dt64), to_unix_timestamp64_nano(dt64);

WITH to_datetime64('2019-09-16 19:20:12.345678910', 9, 'Asia/Istanbul') AS x
SELECT materialize(x) as dt64, to_unix_timestamp64_milli(dt64), to_unix_timestamp64_micro(dt64), to_unix_timestamp64_nano(dt64);

