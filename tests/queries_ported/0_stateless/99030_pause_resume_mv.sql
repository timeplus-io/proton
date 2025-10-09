DROP VIEW IF EXISTS 99030_test_mv;
DROP STREAM IF EXISTS 99030_test_stream;
CREATE STREAM IF NOT EXISTS 99030_test_stream(`id` int);

CREATE MATERIALIZED VIEW 99030_test_mv AS select id from 99030_test_stream;
select sleep(2) format Null;

insert into 99030_test_stream(id) values(1);
system pause materialized view 99030_test_mv;
select sleep(2) format Null;
select id from table(99030_test_mv) order by _tp_time asc;

insert into 99030_test_stream(id) values(2);
select sleep(2) format Null;
select id from table(99030_test_mv) order by _tp_time asc;

system resume materialized view 99030_test_mv;
select sleep(2) format Null;
select id from table(99030_test_mv) order by _tp_time asc;

insert into 99030_test_stream(id) values(3);
select sleep(2) format Null;
select id from table(99030_test_mv) order by _tp_time asc;

select '=== aborted and recovered ===';
system abort materialized view 99030_test_mv;
system recover materialized view 99030_test_mv; --- recover from last paused point
select sleep(3) format Null;
select sleep(3) format Null;
select sleep(3) format Null;
select id from table(99030_test_mv) order by _tp_time asc;

DROP VIEW 99030_test_mv;
DROP STREAM 99030_test_stream;
