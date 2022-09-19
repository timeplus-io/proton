-- Tags: distributed

SET query_mode = 'table';
drop stream if exists merge_distributed;
drop stream if exists merge_distributed1;

create stream merge_distributed1 ( CounterID uint32,  StartDate date,  Sign int8,  VisitID uint64,  UserID uint64,  StartTime DateTime,   ClickLogID uint64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into merge_distributed1 values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create stream merge_distributed ( CounterID uint32,  StartDate date,  Sign int8,  VisitID uint64,  UserID uint64,  StartTime DateTime,   ClickLogID uint64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_distributed1);

alter table merge_distributed1 add column dummy string after CounterID;
alter table merge_distributed add column dummy string after CounterID;

describe table merge_distributed;
show create stream merge_distributed;

insert into merge_distributed1 values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);
select CounterID, dummy from merge_distributed where dummy <> '' limit 10;

alter table merge_distributed drop column dummy;

describe table merge_distributed;
show create stream merge_distributed;

--error: should fall, because there is no `dummy1` column
alter table merge_distributed add column dummy1 string after CounterID;
select CounterID, dummy1 from merge_distributed where dummy1 <> '' limit 10; -- { serverError 47 }

drop stream merge_distributed;
drop stream merge_distributed1;
