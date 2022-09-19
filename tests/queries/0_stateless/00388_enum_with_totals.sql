DROP STREAM IF EXISTS enum_totals;
create stream enum_totals (e Enum8('hello' = 1, 'world' = 2)) ;
INSERT INTO enum_totals VALUES ('hello'), ('world'), ('world');

SELECT e, count() FROM enum_totals GROUP BY e WITH TOTALS ORDER BY e;
DROP STREAM enum_totals;
