--
SELECT 'Invocation with constant';

SELECT to_modified_julian_day('1858-11-16');
SELECT to_modified_julian_day('1858-11-17');
SELECT to_modified_julian_day('2020-11-01');
SELECT to_modified_julian_day(NULL);
SELECT to_modified_julian_day('unparsable'); -- { serverError 27 }
SELECT to_modified_julian_day('1999-02-29'); -- { serverError 38 }
SELECT to_modified_julian_day('1999-13-32'); -- { serverError 38 }

SELECT 'or null';
SELECT to_modified_julian_day_or_null('2020-11-01');
SELECT to_modified_julian_day_or_null('unparsable');
SELECT to_modified_julian_day_or_null('1999-02-29');
SELECT to_modified_julian_day_or_null('1999-13-32');

--
SELECT 'Invocation with string column';

DROP STREAM IF EXISTS to_modified_julian_day_test;
CREATE STREAM to_modified_julian_day_test (d string) ENGINE = Memory;

INSERT INTO to_modified_julian_day_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT to_modified_julian_day(d) FROM to_modified_julian_day_test;

DROP STREAM to_modified_julian_day_test;

--
SELECT 'Invocation with fixed_string column';

DROP STREAM IF EXISTS to_modified_julian_day_test;
CREATE STREAM to_modified_julian_day_test (d fixed_string(10)) ENGINE = Memory;

INSERT INTO to_modified_julian_day_test VALUES ('1858-11-16'), ('1858-11-17'), ('2020-11-01');
SELECT to_modified_julian_day(d) FROM to_modified_julian_day_test;

DROP STREAM to_modified_julian_day_test;
