DROP VIEW IF EXISTS 99029_test_mv;
DROP STREAM IF EXISTS 99029_test_stream;

select 'recover_policy=best_effort This test is expected to skip poison events';

CREATE STREAM IF NOT EXISTS 99029_test_stream(`id` string);

CREATE MATERIALIZED VIEW 99029_test_mv AS select to_int(id) as id, _tp_sn as idx from 99029_test_stream settings recovery_policy='best_effort', checkpoint_interval=1;
select sleep(2) format Null;

insert into 99029_test_stream(id) values(1);
insert into 99029_test_stream(id) values('milet');
select sleep(2) format Null;
insert into 99029_test_stream(id) values('2');
select sleep(2) format Null;

select id, idx from table(99029_test_mv) order by idx asc;

DROP VIEW 99029_test_mv;
DROP STREAM 99029_test_stream;


select 'recover_policy=strict This test is expected to fail with a poison event';

CREATE STREAM IF NOT EXISTS 99029_test_stream(`id` string);

CREATE MATERIALIZED VIEW 99029_test_mv AS select to_int(id) as id, _tp_sn as idx from 99029_test_stream settings recovery_policy='strict', checkpoint_interval=1;
select sleep(2) format Null;

insert into 99029_test_stream(id) values(1);
insert into 99029_test_stream(id) values('milet');
select sleep(3) format Null;
select sleep(3) format Null;
insert into 99029_test_stream(id) values('2');
select sleep(3) format Null;
select sleep(3) format Null;

select id, idx from table(99029_test_mv) order by idx asc;

DROP VIEW 99029_test_mv;
DROP STREAM 99029_test_stream;
