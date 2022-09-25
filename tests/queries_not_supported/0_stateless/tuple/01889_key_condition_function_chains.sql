set force_primary_key=1;
SET query_mode = 'table';

drop stream if exists tab;
create stream tab (t datetime) engine = MergeTree order by to_start_of_day(t);
insert into tab values ('2020-02-02 01:01:01');
select t from tab where t > '2020-01-01 01:01:01';
with t as s select t from tab where s > '2020-01-01 01:01:01';

drop stream if exists tab;
create stream tab (t datetime) engine = MergeTree order by to_start_of_day(t + 1);
insert into tab values ('2020-02-02 01:01:01');
select t from tab where t + 1 > '2020-01-01 01:01:01';
with t + 1 as s select t from tab where s > '2020-01-01 01:01:01';


set force_primary_key = 0;
set force_index_by_date=1;

drop stream if exists tab;
create stream tab (x int32, y int32) engine = MergeTree partition by x + y order by tuple();
insert into tab values (1, 1), (2, 2);
select x, y from tab where (x + y) = 2;
with x + y as s select x, y from tab where s = 2;
-- with x as s select x, y from tab where s + y = 2;

drop stream if exists tab;
create stream tab (x int32, y int32) engine = MergeTree partition by ((x + y) + 1) * 2 order by tuple();
insert into tab values (1, 1), (2, 2);
select x, y from tab where (x + y) + 1 = 3;
-- with x + y as s select x, y from tab where s + 1 = 3;

set force_index_by_date=0;
drop stream if exists tab;
