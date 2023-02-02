SELECT CAST(1111111111.222 AS DateTime64(3, 'Asia/Istanbul'));
SELECT to_datetime(1111111111.222, 3, 'Asia/Istanbul');
SELECT to_datetime64(1111111111.222, 3, 'Asia/Istanbul');

SELECT to_datetime64(0.0, 9, 'UTC') ;
SELECT to_datetime64(0, 9, 'UTC');

SELECT to_datetime64(-2200000000.0, 9, 'UTC'); -- 1900-01-01 < value
SELECT to_datetime64(-2200000000, 9, 'UTC');

SELECT to_datetime64(-2300000000.0, 9, 'UTC'); -- value < 1900-01-01
SELECT to_datetime64(-2300000000, 9, 'UTC');

SELECT to_datetime64(-999999999999.0, 9, 'UTC'); -- value << 1900-01-01
SELECT to_datetime64(-999999999999, 9, 'UTC');

SELECT to_datetime64(9200000000.0, 9, 'UTC'); -- value < 2262-04-11
SELECT to_datetime64(9200000000, 9, 'UTC');

SELECT to_datetime64(9300000000.0, 9, 'UTC'); -- { serverError 407 } # 2262-04-11 < value
SELECT to_datetime64(9300000000, 9, 'UTC'); -- { serverError 407 }

