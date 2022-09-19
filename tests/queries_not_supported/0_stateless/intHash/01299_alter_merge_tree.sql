SET query_mode = 'table';
drop stream if exists merge_tree;

create stream merge_tree ( CounterID uint32,  StartDate date,  Sign int8,  VisitID uint64,  UserID uint64,  StartTime DateTime,   ClickLogID uint64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);

insert into merge_tree values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3)
alter table merge_tree add column dummy string after CounterID;
describe table merge_tree;

insert into merge_tree values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3)

select CounterID, dummy from merge_tree where dummy <> '' limit 10;

alter table merge_tree drop column dummy;

describe table merge_tree;

drop stream merge_tree;
