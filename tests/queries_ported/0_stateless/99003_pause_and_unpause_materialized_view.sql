drop view if exists test_99003_mv;
drop stream if exists test_99003_stream;

create stream test_99003_stream (i int);
create materialized view test_99003_mv as select i from test_99003_stream;

insert into test_99003_stream values (1);
select sleep(1) FORMAT Null;
select i from table(test_99003_mv);

--- Pause the materialized view and insert a new row
pause materialized view test_99003_mv;

insert into test_99003_stream values (2);
select sleep(1) FORMAT Null;
select i from table(test_99003_mv);

--- Unpause the materialized view and insert a new row
unpause materialized view test_99003_mv;
select sleep(1) FORMAT Null;
select i from table(test_99003_mv);

insert into test_99003_stream values (3);
select sleep(1) FORMAT Null;
select i from table(test_99003_mv);

drop view if exists test_99003_mv;
drop stream if exists test_99003_stream;
