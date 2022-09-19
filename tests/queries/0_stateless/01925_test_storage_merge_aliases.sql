-- Tags: no-parallel
SET query_mode = 'table';
drop stream if exists merge;
create stream merge
(
    dt date,
    colAlias0 int32,
    colAlias1 int32,
    col2 int32,
    colAlias2 uint32,
    col3 int32,
    colAlias3 uint32
)
engine = Merge(currentDatabase(), '^alias_');

drop stream if exists alias_1;
drop stream if exists alias_2;

create stream alias_1
(
    dt date,
    col int32,
    colAlias0 uint32 alias col,
    colAlias1 uint32 alias col3 + colAlias0,
    col2 int32,
    colAlias2 int32 alias colAlias1 + col2 + 10,
    col3 int32,
    colAlias3 int32 alias colAlias2 + colAlias1 + col3
)
engine = MergeTree()
order by (dt);

insert into alias_1 (dt, col, col2, col3) values ('2020-02-02', 1, 2, 3);

select 'alias1';
select colAlias0, colAlias1, colAlias2, colAlias3 from alias_1;
select colAlias3, colAlias2, colAlias1, colAlias0 from merge;
select * from merge;

create stream alias_2
(
    dt date,
    col int32,
    col2 int32,
    colAlias0 uint32 alias col,
    colAlias3 int32 alias col3 + colAlias0,
    colAlias1 uint32 alias colAlias0 + col2,
    colAlias2 int32 alias colAlias0 + colAlias1,
    col3 int32
)
engine = MergeTree()
order by (dt);

insert into alias_2 (dt, col, col2, col3) values ('2020-02-01', 1, 2, 3);

select 'alias2';
select colAlias0, colAlias1, colAlias2, colAlias3 from alias_2;
select colAlias3, colAlias2, colAlias1, colAlias0 from merge order by dt;
select * from merge order by dt;
