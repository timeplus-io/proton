SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS enum_totals;
create stream enum_totals (e enum8('hello' = 1, 'world' = 2)) ;
INSERT INTO enum_totals(e) VALUES ('hello'), ('world'), ('world');
SELECT sleep(3);

SELECT e, count() FROM enum_totals GROUP BY e WITH TOTALS ORDER BY e;
DROP STREAM enum_totals;
