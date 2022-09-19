SET query_mode = 'table';
drop stream if exists merge;
drop stream if exists merge1;
drop stream if exists merge2;

create stream merge1 ( CounterID uint32,  StartDate date,  Sign int8,  VisitID uint64,  UserID uint64,  StartTime DateTime,   ClickLogID uint64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into merge1 values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create stream merge2 ( CounterID uint32,  StartDate date,  Sign int8,  VisitID uint64,  UserID uint64,  StartTime DateTime,   ClickLogID uint64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into merge2 values (2, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create stream merge ( CounterID uint32,  StartDate date,  Sign int8,  VisitID uint64,  UserID uint64,  StartTime DateTime,   ClickLogID uint64) ENGINE = Merge(currentDatabase(), 'merge\[0-9\]');

alter table merge1 add column dummy string after CounterID;
alter table merge2 add column dummy string after CounterID;
alter table merge add column dummy string after CounterID;

describe table merge;
show create stream merge;

insert into merge1 values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

select CounterID, dummy from merge where dummy <> '' limit 10;


alter table merge drop column dummy;

describe table merge;
show create stream merge;

--error: must correctly fall into the alter
alter table merge add column dummy1 string after CounterID;
select CounterID, dummy1 from merge where dummy1 <> '' limit 10;

drop stream merge;
drop stream merge1;
drop stream merge2;
