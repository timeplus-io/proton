SET query_mode = 'table';
drop stream if exists ttl_00933_1;

-- Column TTL works only with wide parts, because it's very expensive to apply it for compact parts

create stream ttl_00933_1 (d datetime, a int ttl d + interval 1 second, b int ttl d + interval 1 second) engine = MergeTree order by tuple() partition by to_minute(d) settings min_bytes_for_wide_part = 0;
insert into ttl_00933_1 values (now(), 1, 2);
insert into ttl_00933_1 values (now(), 3, 4);
select sleep(1.1) format Null;
optimize table ttl_00933_1 final;
select a, b from ttl_00933_1;

drop stream if exists ttl_00933_1;

create stream ttl_00933_1 (d datetime, a int, b int)
    engine = MergeTree order by to_date(d) partition by tuple() ttl d + interval 1 second
    settings remove_empty_parts = 0;
insert into ttl_00933_1 values (now(), 1, 2);
insert into ttl_00933_1 values (now(), 3, 4);
insert into ttl_00933_1 values (now() + 1000, 5, 6);
select sleep(1.1) format Null;
optimize table ttl_00933_1 final; -- check ttl merge for part with both expired and unexpired values
select a, b from ttl_00933_1;

drop stream if exists ttl_00933_1;

create stream ttl_00933_1 (d datetime, a int ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by to_day_of_month(d) settings min_bytes_for_wide_part = 0;
insert into ttl_00933_1 values (to_datetime('2000-10-10 00:00:00'), 1);
insert into ttl_00933_1 values (to_datetime('2000-10-10 00:00:00'), 2);
insert into ttl_00933_1 values (to_datetime('2000-10-10 00:00:00'), 3);
optimize table ttl_00933_1 final;
select * from ttl_00933_1 order by d;

drop stream if exists ttl_00933_1;

create stream ttl_00933_1 (d datetime, a int)
    engine = MergeTree order by tuple() partition by tuple() ttl d + interval 1 day
    settings remove_empty_parts = 0;
insert into ttl_00933_1 values (to_datetime('2000-10-10 00:00:00'), 1);
insert into ttl_00933_1 values (to_datetime('2000-10-10 00:00:00'), 2);
insert into ttl_00933_1 values (to_datetime('2100-10-10 00:00:00'), 3);
optimize table ttl_00933_1 final;
select * from ttl_00933_1 order by d;

drop stream if exists ttl_00933_1;

create stream ttl_00933_1 (d date, a int)
    engine = MergeTree order by a partition by to_day_of_month(d) ttl d + interval 1 day
    settings remove_empty_parts = 0;
insert into ttl_00933_1 values (to_date('2000-10-10'), 1);
insert into ttl_00933_1 values (to_date('2100-10-10'), 2);
optimize table ttl_00933_1 final;
select * from ttl_00933_1 order by d;

-- const datetime TTL positive
drop stream if exists ttl_00933_1;
create stream ttl_00933_1 (b int, a int ttl now()-1000) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create stream ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
optimize table ttl_00933_1 final;
select * from ttl_00933_1;

-- const datetime TTL negative
drop stream if exists ttl_00933_1;
create stream ttl_00933_1 (b int, a int ttl now()+1000) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create stream ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
optimize table ttl_00933_1 final;
select * from ttl_00933_1;

-- const date TTL positive
drop stream if exists ttl_00933_1;
create stream ttl_00933_1 (b int, a int ttl today()-1) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create stream ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
optimize table ttl_00933_1 final;
select * from ttl_00933_1;

-- const date TTL negative
drop stream if exists ttl_00933_1;
create stream ttl_00933_1 (b int, a int ttl today()+1) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create stream ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
optimize table ttl_00933_1 final;
select * from ttl_00933_1;

set send_logs_level = 'fatal';

drop stream if exists ttl_00933_1;

create stream ttl_00933_1 (d datetime ttl d) engine = MergeTree order by tuple() partition by to_second(d); -- { serverError 44}
create stream ttl_00933_1 (d datetime, a int ttl d) engine = MergeTree order by a partition by to_second(d); -- { serverError 44}
create stream ttl_00933_1 (d datetime, a int ttl 2 + 2) engine = MergeTree order by tuple() partition by to_second(d); -- { serverError 450 }
create stream ttl_00933_1 (d datetime, a int ttl d - d) engine = MergeTree order by tuple() partition by to_second(d); -- { serverError 450 }

create stream ttl_00933_1 (d datetime, a int  ttl d + interval 1 day) engine = Log; -- { serverError 36 }
create stream ttl_00933_1 (d datetime, a int) engine = Log ttl d + interval 1 day; -- { serverError 36 }

drop stream if exists ttl_00933_1;
