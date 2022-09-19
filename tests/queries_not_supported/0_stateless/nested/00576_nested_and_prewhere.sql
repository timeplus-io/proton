DROP STREAM IF EXISTS nested;

create stream nested (x uint64, filter uint8, n nested(a uint64)) ENGINE = MergeTree ORDER BY x;
INSERT INTO nested SELECT number, number % 2, range(number % 10) FROM system.numbers LIMIT 100000;

ALTER STREAM nested ADD COLUMN n.b array(uint64);
SELECT DISTINCT n.b FROM nested PREWHERE filter;

ALTER STREAM nested ADD COLUMN n.c array(uint64) DEFAULT array_map(x -> x * 2, n.a);
SELECT DISTINCT n.c FROM nested PREWHERE filter;
SELECT DISTINCT n.a, n.c FROM nested PREWHERE filter;

DROP STREAM nested;
