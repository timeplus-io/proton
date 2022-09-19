DROP STREAM IF EXISTS x_1;
DROP STREAM IF EXISTS x_2;
DROP STREAM IF EXISTS x;

create stream x_1 engine=Log as select * from numbers(10);
create stream x_2 engine=Log as select * from numbers(10);
create stream x engine=Merge(currentDatabase(), '^x_(1|2)$') as x_1;

select _table, count() from x group by _table order by _table;

DROP STREAM x_1;
DROP STREAM x_2;
DROP STREAM x;
