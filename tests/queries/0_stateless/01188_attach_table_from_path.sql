-- Tags: no-replicated-database, no-parallel
SET query_mode = 'table';
drop stream if exists test;
drop stream if exists file;
drop stream if exists mt;

attach table test from 'some/path' (n uint8) engine=Memory; -- { serverError 48 }
attach table test from '/etc/passwd' (s string) engine=File(TSVRaw); -- { serverError 481 }
attach table test from '../../../../../../../../../etc/passwd' (s string) engine=File(TSVRaw); -- { serverError 481 }
attach table test from 42 (s string) engine=File(TSVRaw); -- { clientError 62 }

insert into table function file('01188_attach/file/data.TSV', 'TSV', 's string, n uint8') values ('file', 42);
attach table file from '01188_attach/file' (s string, n uint8) engine=File(TSV);
select * from file;
detach table file;
attach table file;
select * from file;

attach table mt from '01188_attach/file' (n uint8, s string) engine=MergeTree order by n;
select * from mt;
insert into mt values (42, 'mt');
select * from mt;
detach table mt;
attach table mt;
select * from mt;

drop stream file;
drop stream mt;
