DROP STREAM IF EXISTS prewhere;

create stream prewhere (d date, a string, b string) ENGINE = MergeTree(d, d, 8192);
INSERT INTO prewhere VALUES ('2015-01-01', 'hello', 'world');

ALTER STREAM prewhere ADD COLUMN a1 string AFTER a;
INSERT INTO prewhere VALUES ('2015-01-01', 'hello1', 'xxx', 'world1');

SELECT d, a, a1, b FROM prewhere PREWHERE a LIKE 'hello%' ORDER BY a1;

DROP STREAM prewhere;
