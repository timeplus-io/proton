--
SELECT 'Invocation with constant';

SELECT fromModifiedJulianDay(-1);
SELECT fromModifiedJulianDay(0);
SELECT fromModifiedJulianDay(59154);
SELECT fromModifiedJulianDay(NULL);
SELECT fromModifiedJulianDay(CAST(NULL, 'Nullable(int64)'));
SELECT fromModifiedJulianDay(-678942); -- { serverError 490 }
SELECT fromModifiedJulianDay(2973484); -- { serverError 490 }

SELECT 'or null';
SELECT fromModifiedJulianDayOrNull(59154);
SELECT fromModifiedJulianDayOrNull(NULL);
SELECT fromModifiedJulianDayOrNull(-678942);
SELECT fromModifiedJulianDayOrNull(2973484);

--
SELECT 'Invocation with int32 column';

DROP STREAM IF EXISTS fromModifiedJulianDay_test;
create stream fromModifiedJulianDay_test (d int32) ;

INSERT INTO fromModifiedJulianDay_test VALUES (-1), (0), (59154);
SELECT fromModifiedJulianDay(d) FROM fromModifiedJulianDay_test;

DROP STREAM fromModifiedJulianDay_test;
