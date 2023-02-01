-- Error cases
SELECT to_start_of_second('123');  -- {serverError 43}
SELECT to_start_of_second(now());  -- {serverError 43}
SELECT to_start_of_second();   -- {serverError 42}
SELECT to_start_of_second(now64(), 123);   -- {serverError 43}

WITH to_datetime64('2019-09-16 19:20:11', 3, 'Asia/Istanbul') AS dt64 SELECT to_start_of_second(dt64, 'UTC') AS res, to_type_name(res);
WITH to_datetime64('2019-09-16 19:20:11', 0, 'UTC') AS dt64 SELECT to_start_of_second(dt64) AS res, to_type_name(res);
WITH to_datetime64('2019-09-16 19:20:11.123', 3, 'UTC') AS dt64 SELECT to_start_of_second(dt64) AS res, to_type_name(res);
WITH to_datetime64('2019-09-16 19:20:11.123', 9, 'UTC') AS dt64 SELECT to_start_of_second(dt64) AS res, to_type_name(res);

SELECT 'non-const column';
WITH to_datetime64('2019-09-16 19:20:11.123', 3, 'UTC') AS dt64 SELECT to_start_of_second(materialize(dt64)) AS res, to_type_name(res);
