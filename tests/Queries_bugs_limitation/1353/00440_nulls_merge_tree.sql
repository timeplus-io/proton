SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS nulls;
create stream nulls (d date, x nullable(uint64)) ENGINE = MergeTree(d, d, 8192);
INSERT INTO nulls(d, x) SELECT to_date('2000-01-01'), number % 10 != 0 ? number : NULL FROM system.numbers LIMIT 10000;
SELECT sleep(3);
SELECT count() FROM nulls WHERE x IS NULL;
DROP STREAM nulls;
