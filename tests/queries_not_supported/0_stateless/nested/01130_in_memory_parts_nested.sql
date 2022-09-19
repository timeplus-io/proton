-- Test 00576_nested_and_prewhere, but with in-memory parts.
DROP STREAM IF EXISTS nested;

create stream nested (x uint64, filter uint8, n nested(a uint64)) ENGINE = MergeTree ORDER BY x
    SETTINGS min_rows_for_compact_part = 200000, min_rows_for_wide_part = 300000;

INSERT INTO nested SELECT number, number % 2, range(number % 10) FROM system.numbers LIMIT 100000;

ALTER STREAM nested ADD COLUMN n.b array(uint64);
SELECT DISTINCT n.b FROM nested PREWHERE filter;
SELECT DISTINCT n.b FROM nested PREWHERE filter SETTINGS max_block_size = 10;
SELECT DISTINCT n.b FROM nested PREWHERE filter SETTINGS max_block_size = 123;

ALTER STREAM nested ADD COLUMN n.c array(uint64) DEFAULT array_map(x -> x * 2, n.a);
SELECT DISTINCT n.c FROM nested PREWHERE filter;
SELECT DISTINCT n.a, n.c FROM nested PREWHERE filter;

DROP STREAM nested;
