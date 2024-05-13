drop view if exists test_99003_mv;
drop stream if exists test_99003_stream;

create stream test_99003_stream (i int);
create materialized view test_99003_mv as select i from test_99003_stream;
select sleep(1) FORMAT Null;

insert into test_99003_stream(i) values (1);
select sleep(1) FORMAT Null;
select i from table(test_99003_mv) order by i;

--- Pause the materialized view and insert a new row
PAUSE MATERIALIZED VIEW test_99003_mv SYNC;

select '---';
insert into test_99003_stream(i) values (2);
select sleep(1) FORMAT Null;
select i from table(test_99003_mv) order by i;

--- Unpause the materialized view and insert a new row
UNPAUSE MATERIALIZED VIEW test_99003_mv SYNC;

select '---';
select i from table(test_99003_mv) order by i;

select '---';
insert into test_99003_stream(i) values (3);
select sleep(1) FORMAT Null;
select i from table(test_99003_mv) order by i;

drop view if exists test_99003_mv;
drop stream if exists test_99003_stream;
