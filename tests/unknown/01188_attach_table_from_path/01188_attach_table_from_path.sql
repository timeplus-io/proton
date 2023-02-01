-- Tags: no-replicated-database, no-parallel

drop stream if exists test;
drop stream if exists file;
drop stream if exists mt;

attach stream test from 'some/path' (n uint8) engine=Memory; -- { serverError 48 }
attach stream test from '/etc/passwd' (s string) engine=File(TSVRaw); -- { serverError 481 }
attach stream test from '../../../../../../../../../etc/passwd' (s string) engine=File(TSVRaw); -- { serverError 481 }
attach stream test from 42 (s string) engine=File(TSVRaw); -- { clientError 62 }

insert into stream function file('01188_attach/file/data.TSV', 'TSV', 's string, n uint8') values ('file', 42);
attach stream file from '01188_attach/file' (s string, n uint8) engine=File(TSV);
select * from file;
detach stream file;
attach stream file;
select * from file;

attach stream mt from '01188_attach/file' (n uint8, s string) engine=MergeTree order by n;
select * from mt;
insert into mt values (42, 'mt');
select * from mt;
detach stream mt;
attach stream mt;
select * from mt;

drop stream file;
drop stream mt;
