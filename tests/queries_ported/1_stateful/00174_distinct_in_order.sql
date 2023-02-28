select '-- check that distinct with and w/o optimization produce the same result';

drop stream if exists distinct_in_order sync;
drop stream if exists ordinary_distinct sync;

select '-- DISTINCT colums are the same as in ORDER BY';
create stream distinct_in_order (CounterID uint32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into distinct_in_order select distinct CounterID, EventDate from test.hits order by CounterID, EventDate settings optimize_distinct_in_order=1;
create stream ordinary_distinct (CounterID uint32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into ordinary_distinct select distinct CounterID, EventDate from test.hits order by CounterID, EventDate settings optimize_distinct_in_order=0;
select distinct * from distinct_in_order except select * from ordinary_distinct;

drop stream if exists distinct_in_order sync;
drop stream if exists ordinary_distinct sync;

select '-- DISTINCT colums has prefix in ORDER BY columns';
create stream distinct_in_order (CounterID uint32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into distinct_in_order select distinct CounterID, EventDate from test.hits order by CounterID settings optimize_distinct_in_order=1;
create stream ordinary_distinct (CounterID uint32, EventDate Date) engine=MergeTree() order by (CounterID, EventDate);
insert into ordinary_distinct select distinct CounterID, EventDate from test.hits order by CounterID settings optimize_distinct_in_order=0;
select distinct * from distinct_in_order except select * from ordinary_distinct;

drop stream if exists distinct_in_order sync;
drop stream if exists ordinary_distinct sync;
