

SELECT '**** constant-constant comparisons ****';

SELECT 'ab\0c' < 'ab\0d', 'ab\0c' > 'ab\0d';
SELECT 'a' < 'a\0b', 'a' > 'a\0b';
SELECT 'a\0\0\0\0' < 'a\0\0\0', 'a\0\0\0\0' > 'a\0\0\0';

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS strings_00469;
create stream strings_00469(x string, y string) ;

INSERT INTO strings_00469(x, y) VALUES ('abcde\0', 'abcde'), ('aa\0a', 'aa\0b'), ('aa', 'aa\0'), ('a\0\0\0\0', 'a\0\0\0'), ('a\0\0', 'a\0'), ('a', 'a');
SELECT sleep(3);
SELECT '**** vector-vector comparisons ****';

SELECT x < y, x > y FROM strings_00469;

SELECT '**** vector-constant comparisons ****';

SELECT x < 'aa', x > 'aa' FROM strings_00469;

SELECT '****';

SELECT x < 'a\0', x > 'a\0' FROM strings_00469;

SELECT '**** single-column sort ****'; -- Uses ColumnString::getPermutation()

SELECT * FROM strings_00469 ORDER BY x;

SELECT '**** multi-column sort ****'; -- Uses ColumnString::compareAt()

SELECT * FROM strings_00469 ORDER BY x, y;

DROP STREAM strings_00469;
