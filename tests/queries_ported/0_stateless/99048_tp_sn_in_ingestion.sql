drop view if exists 99048_mv;
drop view if exists 99048_mv_without_tp_sn;
drop view if exists 99048_mv_without_tp_time;
drop view if exists 99048_mv_without_id;

drop stream if exists 99048_src;
drop stream if exists 99048_dst;

create stream 99048_src(id uint64) comment 'Source stream for testing';
create stream 99048_dst(id uint64) comment 'Destination stream for testing';

select sleep(2) format Null;

insert into 99048_src(id) values(1)(2);
select sleep(1) format Null;

select '=== Initial state of destination stream ===';
select count() from table(99048_dst);

insert into 99048_dst select * from table(99048_src);
select sleep(2) format Null;

select '=== State after ingestion ===';
select count() from table(99048_dst);

select '=== Testing _tp_sn restriction ===';
-- The following should fail with error NUMBER_OF_COLUMNS_DOESNT_MATCH(20) since _tp_sn cannot be set manually in ingestion
insert into 99048_dst select 1 as id, now64() as _tp_time, 3 as _tp_sn; -- { serverError 20 }
select sleep(1) format Null;

select '=== State after testing _tp_sn restriction ===';
select count() from table(99048_dst);




create materialized view 99048_mv as select * from 99048_dst;
create materialized view 99048_mv_without_tp_sn as select id, _tp_time from 99048_dst;
create materialized view 99048_mv_without_tp_time as select id, _tp_sn from 99048_dst;
create materialized view 99048_mv_without_id as select _tp_time, _tp_sn from 99048_dst;

select sleep(2) format Null;
insert into 99048_dst select number, now64() from numbers(7);
select sleep(1) format Null;

select '=== MV State after ingesting data with all columns ===';
select count() from table(99048_mv);

select '=== MV State after ingesting data without _tp_sn ===';
select count() from table(99048_mv_without_tp_sn);

select '=== MV State after ingesting data without _tp_time ===';
select count() from table(99048_mv_without_tp_time);

select '=== MV State after ingesting data without id ===';
select count() from table(99048_mv_without_id);
