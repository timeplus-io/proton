-- Tags: no-parallel

SET query_mode = 'table';
drop stream if exists ttl_00933_2;

create stream ttl_00933_2 (d datetime, a int default 111 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by to_day_of_month(d);
insert into ttl_00933_2 values (to_datetime('2000-10-10 00:00:00'), 1);
insert into ttl_00933_2 values (to_datetime('2000-10-10 00:00:00'), 2);
insert into ttl_00933_2 values (to_datetime('2100-10-10 00:00:00'), 3);
insert into ttl_00933_2 values (to_datetime('2100-10-10 00:00:00'), 4);
optimize table ttl_00933_2 final;
select a from ttl_00933_2 order by a;

drop stream if exists ttl_00933_2;

create stream ttl_00933_2 (d datetime, a int, b default a * 2 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by to_day_of_month(d);
insert into ttl_00933_2 values (to_datetime('2000-10-10 00:00:00'), 1, 100);
insert into ttl_00933_2 values (to_datetime('2000-10-10 00:00:00'), 2, 200);
insert into ttl_00933_2 values (to_datetime('2100-10-10 00:00:00'), 3, 300);
insert into ttl_00933_2 values (to_datetime('2100-10-10 00:00:00'), 4, 400);
optimize table ttl_00933_2 final;
select a, b from ttl_00933_2 order by a;

drop stream if exists ttl_00933_2;

create stream ttl_00933_2 (d datetime, a int, b default 222 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by to_day_of_month(d);
insert into ttl_00933_2 values (to_datetime('2000-10-10 00:00:00'), 1, 5);
insert into ttl_00933_2 values (to_datetime('2000-10-10 00:00:00'), 2, 10);
insert into ttl_00933_2 values (to_datetime('2100-10-10 00:00:00'), 3, 15);
insert into ttl_00933_2 values (to_datetime('2100-10-10 00:00:00'), 4, 20);
optimize table ttl_00933_2 final;
select a, b from ttl_00933_2 order by a;

drop stream if exists ttl_00933_2;
