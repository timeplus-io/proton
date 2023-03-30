-- Tags: no-parallel

drop stream if exists ttl;

create stream ttl (d Date, a int) engine = MergeTree order by a partition by to_day_of_month(d) settings remove_empty_parts = 0;
insert into ttl values (to_datetime('2000-10-10 00:00:00'), 1);
insert into ttl values (to_datetime('2000-10-10 00:00:00'), 2);
insert into ttl values (to_datetime('2100-10-10 00:00:00'), 3);
insert into ttl values (to_datetime('2100-10-10 00:00:00'), 4);

alter stream ttl modify ttl d + interval 1 day;

select sleep(1) format Null; -- wait if very fast merge happen
optimize table ttl partition 10 final;

select * from ttl order by d, a;

drop stream if exists ttl;
