--
SELECT 'Invocation with constant';

SELECT toModifiedJulianDay('1858-11-16');
SELECT toModifiedJulianDay('1858-11-17');
SELECT toModifiedJulianDay('2020-11-01');
SELECT toModifiedJulianDay(NULL);
SELECT toModifiedJulianDay('unparsable'); -- { serverError 27 }
SELECT toModifiedJulianDay('1999-02-29'); -- { serverError 38 }
SELECT toModifiedJulianDay('1999-13-32'); -- { serverError 38 }

SELECT 'or null';
SELECT toModifiedJulianDayOrNull('2020-11-01');
SELECT toModifiedJulianDayOrNull('unparsable');
SELECT toModifiedJulianDayOrNull('1999-02-29');
SELECT toModifiedJulianDayOrNull('1999-13-32');

--
SELECT 'Invocation with string column';

DROP STREAM IF EXISTS toModifiedJulianDay_test;
create stream toModifiedJulianDay_test (d string) ;

INSERT INTO toModifiedJulianDay_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT toModifiedJulianDay(d) FROM toModifiedJulianDay_test;

DROP STREAM toModifiedJulianDay_test;

--
SELECT 'Invocation with FixedString column';

DROP STREAM IF EXISTS toModifiedJulianDay_test;
create stream toModifiedJulianDay_test (d FixedString(10)) ;

INSERT INTO toModifiedJulianDay_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT toModifiedJulianDay(d) FROM toModifiedJulianDay_test;

DROP STREAM toModifiedJulianDay_test;
