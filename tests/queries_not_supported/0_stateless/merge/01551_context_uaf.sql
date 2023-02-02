DROP STREAM IF EXISTS f;
DROP STREAM IF EXISTS v;

create stream f(s string) engine File(TSV, '/dev/null');
create view v as (select * from f);
select * from v; -- was failing long time ago
select * from merge('', 'f'); -- was failing long time ago

DROP STREAM f;
DROP STREAM v;
