show tables from information_schema;
SHOW TABLES FROM INFORMATION_SCHEMA;

SET query_mode = 'table';
create stream t (n uint64, f Float32, s string, fs FixedString(42), d Decimal(9, 6)) engine=Memory;
create view v (n Nullable(int32), f float64) as select n, f from t;
create materialized view mv engine=Null as select * from system.one;
create temporary table tmp (d date, dt DateTime, dtms DateTime64(3));

-- FIXME #28687
select * from information_schema.schemata where schema_name ilike 'information_schema';
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema=currentDatabase() OR table_schema='') AND table_name NOT LIKE '%inner%';
select * from information_schema.views where table_schema=currentDatabase();
-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema=currentDatabase() OR table_schema='') AND table_name NOT LIKE '%inner%';

drop stream t;
drop view v;
drop view mv;
