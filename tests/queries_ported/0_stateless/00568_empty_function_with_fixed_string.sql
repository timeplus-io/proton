SELECT to_fixed_string('', 4) AS str, empty(str) AS is_empty;
SELECT to_fixed_string('\0abc', 4) AS str, empty(str) AS is_empty;

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS defaulted;
create stream defaulted (v6 fixed_string(16)) ENGINE=Memory;
INSERT INTO defaulted(v6) SELECT to_fixed_string('::0', 16) FROM numbers(32768);
SELECT sleep(3);
SELECT count(), not_empty(v6) as e FROM defaulted GROUP BY e;
DROP STREAM defaulted;
