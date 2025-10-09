-- Tags: disabled

-- we do not fully suppport address sanitizer + argMax/argMin compile see #9223

drop view if exists 99072_mv;
drop stream if exists 99072_stream;

create stream 99072_stream(id int, code nullable(string));

create materialized view 99072_mv as select min(id) as min_id, max(id) as max_id, arg_min(code, id), arg_max(code, id), arg_min(id, code), arg_max(id, code), arg_min(code, code), arg_max(code, code) from 99072_stream emit on update;

select sleep(2) format Null;
insert into 99072_stream(id, code) values (1, null);
insert into 99072_stream(id, code) values (2, 'test');
insert into 99072_stream(id, code) values (3, null);
insert into 99072_stream(id, code) values (4, 'test2');
insert into 99072_stream(id, code) values (5, null);

select sleep(3) format Null;

select min(id) as min_id, max(id) as max_id, arg_min(code, id), arg_max(code, id), arg_min(id, code), arg_max(id, code), arg_min(code, code), arg_max(code, code) from table(99072_stream);
select '============';
select * except _tp_time from table(99072_mv) order by _tp_sn;

drop view if exists 99072_mv;
drop stream if exists 99072_stream;
