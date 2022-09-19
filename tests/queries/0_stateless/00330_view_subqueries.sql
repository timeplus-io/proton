SET query_mode = 'table';
drop stream IF EXISTS v1;
drop stream IF EXISTS v2;

CREATE VIEW v1 AS SELECT 1 FROM (SELECT 1);
SELECT * FROM v1;

CREATE VIEW v2 AS SELECT number * number FROM (SELECT number FROM system.numbers LIMIT 10);
SELECT * FROM v2;

drop stream v1;
drop stream v2;
