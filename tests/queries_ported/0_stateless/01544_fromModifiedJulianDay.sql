--
SELECT 'Invocation with constant';

SELECT from_modified_julian_day(-1);
SELECT from_modified_julian_day(0);
SELECT from_modified_julian_day(59154);
SELECT from_modified_julian_day(NULL);
SELECT from_modified_julian_day(CAST(NULL, 'nullable(int64)'));
SELECT from_modified_julian_day(-678942); -- { serverError 490 }
SELECT from_modified_julian_day(2973484); -- { serverError 490 }

SELECT 'or null';
SELECT from_modified_julian_day_or_null(59154);
SELECT from_modified_julian_day_or_null(NULL);
SELECT from_modified_julian_day_or_null(-678942);
SELECT from_modified_julian_day_or_null(2973484);

--
SELECT 'Invocation with int32 column';

DROP STREAM IF EXISTS from_modified_julian_day_test;
CREATE STREAM from_modified_julian_day_test (d int32) ENGINE = Memory;

INSERT INTO from_modified_julian_day_test VALUES (-1), (0), (59154);
SELECT from_modified_julian_day(d) FROM from_modified_julian_day_test;

DROP STREAM from_modified_julian_day_test;
