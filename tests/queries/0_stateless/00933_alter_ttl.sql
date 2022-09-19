-- Tags: no-parallel

set send_logs_level = 'fatal';
SET query_mode = 'table';
drop stream if exists ttl;

create stream ttl (d date, a int) engine = MergeTree order by a partition by to_day_of_month(d) settings remove_empty_parts = 0;
alter stream ttl modify ttl d + interval 1 day;
show create stream ttl;
insert into ttl values (to_datetime('2000-10-10 00:00:00'), 1);
insert into ttl values (to_datetime('2000-10-10 00:00:00'), 2);
insert into ttl values (to_datetime('2100-10-10 00:00:00'), 3);
insert into ttl values (to_datetime('2100-10-10 00:00:00'), 4);
optimize table ttl partition 10 final;

select * from ttl order by d;

alter stream ttl modify ttl a; -- { serverError 450 }

drop stream if exists ttl;

create stream ttl (d date, a int) engine = MergeTree order by tuple() partition by to_day_of_month(d) settings remove_empty_parts = 0;
alter stream ttl modify column a int ttl d + interval 1 day;
desc stream ttl;
alter stream ttl modify column d int ttl d + interval 1 day; -- { serverError 43 }
alter stream ttl modify column d DateTime ttl d + interval 1 day; -- { serverError 524 }

drop stream if exists ttl;
