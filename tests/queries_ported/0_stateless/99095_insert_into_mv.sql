drop view if exists 99095_mv;
drop view if exists 99095_mv_with_target;
drop stream if exists 99095_stream;
drop stream if exists 99095_target_stream;

create stream 99095_stream(id int);
create stream 99095_target_stream(id int);
CREATE MATERIALIZED VIEW 99095_mv AS select * from 99095_stream STORAGE_SETTINGS flush_threshold_count=1;
CREATE MATERIALIZED VIEW 99095_mv_with_target into 99095_target_stream AS select * from 99095_stream STORAGE_SETTINGS flush_threshold_count=1;

insert into 99095_mv(id) values(1); -- { serverError UNSUPPORTED }
insert into 99095_mv_with_target(id) values(1); -- { serverError UNSUPPORTED }

insert into 99095_mv(id) settings allow_insert_into_materialized_view=true values(1);
insert into 99095_mv_with_target(id) settings allow_insert_into_materialized_view=true values(1); -- { serverError UNSUPPORTED }

select sleep(2) format Null;
select '===== 99095_mv =====';
select * except _tp_time from table(99095_mv);
select '===== 99095_mv_with_target =====';
select * except _tp_time from table(99095_mv_with_target);

drop view if exists 99095_mv;
drop view if exists 99095_mv_with_target;
drop stream if exists 99095_stream;
drop stream if exists 99095_target_stream;
