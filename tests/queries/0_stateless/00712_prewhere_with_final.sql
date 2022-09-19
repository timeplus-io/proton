SET query_mode = 'table';
drop stream if exists trepl;
create stream trepl(d date,a int32, b int32) engine = ReplacingMergeTree(d, (a,b), 8192);
insert into trepl values ('2018-09-19', 1, 1);
select b from trepl FINAL prewhere a < 1000;
drop stream trepl;


drop stream if exists versioned_collapsing;
create stream versioned_collapsing(d date, x uint32, sign int8, version uint32) engine = VersionedCollapsingMergeTree(d, x, 8192, sign, version);
insert into versioned_collapsing values ('2018-09-19', 123, 1, 0);
select x from versioned_collapsing FINAL prewhere version < 1000;
drop stream versioned_collapsing;
