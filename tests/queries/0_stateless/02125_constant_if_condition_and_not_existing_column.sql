SET query_mode = 'table';
drop stream if exists test;
-- this queries does not have to pass, but they works historically
-- let's support this while can, see #31687
create stream test (x string) Engine=StripeLog;
insert into test values (0);
select if(0, y, 42) from test;
select if(1, 42, y) from test;
select if(to_uint8(0), y, 42) from test;
select if(to_int8(0), y, 42) from test;
select if(to_uint8(1), 42, y) from test;
select if(to_int8(1), 42, y) from test;
select if(to_uint8(to_uint8(0)), y, 42) from test;
select if(cast(cast(0, 'uint8'), 'uint8'), y, 42) from test;
explain syntax select x, if((select hasColumnInTable(currentDatabase(), 'test', 'y')), y, x || '_')  from test;
drop stream if exists t;
