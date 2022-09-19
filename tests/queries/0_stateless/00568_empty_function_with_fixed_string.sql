SELECT to_fixed_string('', 4) AS str, empty(str) AS is_empty;
SELECT to_fixed_string('\0abc', 4) AS str, empty(str) AS is_empty;

DROP STREAM IF EXISTS defaulted;
create stream defaulted (v6 FixedString(16)) ENGINE=Memory;
INSERT INTO defaulted SELECT to_fixed_string('::0', 16) FROM numbers(32768);
SELECT count(), not_empty(v6) e FROM defaulted GROUP BY e;
DROP STREAM defaulted;
