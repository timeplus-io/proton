-- Tags: no-parallel
SET query_mode = 'table';
drop stream if exists ttl;

create stream ttl (d date, a int) engine = MergeTree order by a partition by to_day_of_month(d);
insert into ttl values (to_datetime('2000-10-10 00:00:00'), 1);
insert into ttl values (to_datetime('2000-10-10 00:00:00'), 2);
insert into ttl values (to_datetime('2100-10-10 00:00:00'), 3);
insert into ttl values (to_datetime('2100-10-10 00:00:00'), 4);

set mutations_sync = 2;

alter stream ttl modify ttl d + interval 1 day;
select * from ttl order by a;
select '=============';

drop stream if exists ttl;

create stream ttl (i int, s string) engine = MergeTree order by i;
insert into ttl values (1, 'a') (2, 'b') (3, 'c') (4, 'd');

alter stream ttl modify ttl i % 2 = 0 ? today() - 10 : to_date('2100-01-01');
select * from ttl order by i;
select '=============';

alter stream ttl modify ttl to_date('2000-01-01');
select * from ttl order by i;
select '=============';

drop stream if exists ttl;

create stream ttl (i int, s string) engine = MergeTree order by i;
insert into ttl values (1, 'a') (2, 'b') (3, 'c') (4, 'd');

alter stream ttl modify column s string ttl i % 2 = 0 ? today() - 10 : to_date('2100-01-01');
select * from ttl order by i;
select '=============';

alter stream ttl modify column s string ttl to_date('2000-01-01');
select * from ttl order by i;
select '=============';

drop stream if exists ttl;

create stream ttl (d date, i int, s string) engine = MergeTree order by i;
insert into ttl values (to_date('2000-01-02'), 1, 'a') (to_date('2000-01-03'), 2, 'b') (to_date('2080-01-01'), 3, 'c') (to_date('2080-01-03'), 4, 'd');

alter stream ttl modify ttl i % 3 = 0 ? today() - 10 : to_date('2100-01-01');
select i, s from ttl order by i;
select '=============';

alter stream ttl modify column s string ttl d + interval 1 month;
select i, s from ttl order by i;
select '=============';

drop stream if exists ttl;

create stream ttl (i int, s string, t string) engine = MergeTree order by i;
insert into ttl values (1, 'a', 'aa') (2, 'b', 'bb') (3, 'c', 'cc') (4, 'd', 'dd');

alter stream ttl modify column s string ttl i % 3 = 0 ? today() - 10 : to_date('2100-01-01'),
                modify column t string ttl i % 3 = 1 ? today() - 10 : to_date('2100-01-01');

select i, s, t from ttl order by i;
-- MATERIALIZE TTL ran only once
select count() from system.mutations where database = currentDatabase() and table = 'ttl' and is_done;
select '=============';

drop stream if exists ttl;

-- Nothing changed, don't run mutation
create stream ttl (i int, s string ttl to_date('2000-01-02')) engine = MergeTree order by i;
alter stream ttl modify column s string ttl to_date('2000-01-02');
select count() from system.mutations where database = currentDatabase() and table = 'ttl' and is_done;

drop stream if exists ttl;
