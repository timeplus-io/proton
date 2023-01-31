drop stream if exists alter_ttl;

create stream alter_ttl(i int) engine = MergeTree order by i ttl to_date('2020-05-05');
alter stream alter_ttl add column s string;
alter stream alter_ttl modify column s string ttl to_date('2020-01-01');
show create stream alter_ttl;
drop stream alter_ttl;

create stream alter_ttl(d date, s string) engine = MergeTree order by d ttl d + interval 1 month;
alter stream alter_ttl modify column s string ttl d + interval 1 day;
show create stream alter_ttl;
drop stream alter_ttl;
