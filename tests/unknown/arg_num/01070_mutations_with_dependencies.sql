-- Tags: no-parallel, no-s3-storage
-- With s3 policy TTL TO DISK 'default' doesn't work (because we have no default, only 's3')

drop stream if exists ttl;
set mutations_sync = 2;

-- check that ttl info was updated after mutation.
create stream ttl (i int, a int, s string) engine = MergeTree order by i;
insert into ttl values (1, 1, 'a') (2, 1, 'b') (3, 1, 'c') (4, 1, 'd');

alter stream ttl modify ttl a % 2 = 0 ? today() - 10 : toDate('2100-01-01');
alter stream ttl materialize ttl;

select * from ttl order by i;
alter stream ttl update a = 0 where i % 2 = 0;
select * from ttl order by i;

drop stream ttl;

select '===================';

-- check that skip index is updated after column was modified by ttl.
create stream ttl (i int, a int, s string default 'b' ttl a % 2 = 0 ? today() - 10 : toDate('2100-01-01'),
    index ind_s (s) type set(1) granularity 1) engine = MergeTree order by i;
insert into ttl values (1, 1, 'a') (2, 1, 'a') (3, 1, 'a') (4, 1, 'a');

select count() from ttl where s = 'a';

alter stream ttl update a = 0 where i % 2 = 0;

select count() from ttl where s = 'a';
select count() from ttl where s = 'b';

drop stream ttl;

-- check only that it doesn't throw exceptions.
create stream ttl (i int, s string) engine = MergeTree order by i ttl toDate('2000-01-01') TO DISK 'default';
alter stream ttl materialize ttl;
drop stream ttl;

create stream ttl (a int, b int, c int default 42 ttl d, d Date, index ind (b * c) type minmax granularity 1)
engine = MergeTree order by a;
insert into ttl values (1, 2, 3, '2100-01-01');
alter stream ttl update d = '2000-01-01' where 1;
alter stream ttl materialize ttl;
select * from ttl;
drop stream ttl;
