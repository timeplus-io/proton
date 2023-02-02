SET joined_subquery_requires_alias = 0;
SET max_threads = 1;

drop stream if exists tab1;
drop stream if exists tab2;

create stream tab1 (a1 int32, b1 int32) engine = MergeTree order by a1;
create stream tab2 (a2 int32, b2 int32) engine = MergeTree order by a2;

insert into tab1 values (1, 2);
insert into tab2 values (2, 3);
insert into tab2 values (6, 4);

select 'subqueries with OR';
select a1 from tab1 any left join (select * from tab2) on b1 = a2 or b2 = a1;
select '==';
select a1 from tab1 any left join (select a2, b2 from tab2) on b1 = a2 or b2 = a1;
select '==';
select a1, b1 from tab1 any left join (select * from tab2) on b1 = a2 or b2 = a1;

select 'subquery column alias with OR';
select a1, b1, a2, b2 from tab1 any left join (select *, a2 as z from tab2) on b1 + 1 = z + 1 or b1 = z * 2;
select '==';
select a1, b1, a2, b2 from tab1 any left join (select *, a2 + 1 as z from tab2) on b1 + 1 = z or b1 = z * 2;

drop stream tab1;
drop stream tab2;
