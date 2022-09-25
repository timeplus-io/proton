-- Tags: no-parallel
SET query_mode = 'table';
drop stream IF EXISTS union;

create view union as select 1 as test union all select 2;

SELECT * FROM union ORDER BY test;

DETACH STREAM union;
ATTACH STREAM union;

SELECT * FROM union ORDER BY test;

drop stream union;
