DROP STREAM IF EXISTS nulls;
create stream nulls (d date, x Nullable(uint64)) ENGINE = MergeTree(d, d, 8192);
INSERT INTO nulls SELECT to_date('2000-01-01'), number % 10 != 0 ? number : NULL FROM system.numbers LIMIT 10000;
SELECT count() FROM nulls WHERE x IS NULL;
DROP STREAM nulls;
