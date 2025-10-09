drop view if exists 99017_mv1;
drop view if exists 99017_mv2;
drop stream if exists 99017_target_stream;
drop stream if exists 99017_source_stream;

CREATE STREAM 99017_target_stream(`id` int32);
CREATE STREAM 99017_source_stream(`id` int32, `value` int32);
select sleep(1) format Null;

CREATE MATERIALIZED VIEW 99017_mv1 INTO 99017_target_stream AS SELECT id, value from 99017_source_stream;
CREATE MATERIALIZED VIEW 99017_mv2 INTO 99017_target_stream AS SELECT value from 99017_source_stream; -- { serverError INCORRECT_QUERY }

insert into 99017_source_stream(id, value) values (1, 1);
select sleep(2) format Null;

select * except _tp_time from table(99017_mv1);
select * except _tp_time from 99017_mv1 settings query_mode='table';
select * except _tp_time from 99017_mv1 where _tp_time > earliest_ts() limit 1;

drop view if exists 99017_mv1;
drop view if exists 99017_mv2;
drop stream if exists 99017_target_stream;
drop stream if exists 99017_source_stream;
