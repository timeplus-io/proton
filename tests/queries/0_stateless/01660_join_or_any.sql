SET joined_subquery_requires_alias = 0;
SET any_join_distinct_right_table_keys = 0;

SET query_mode = 'table';
drop stream if exists tab1;
drop stream if exists tab2;
drop stream if exists tab3;

create stream tab1 (a1 int32, b1 int32) engine = MergeTree order by a1;
create stream tab2 (a2 int32, b2 int32) engine = MergeTree order by a2;
create stream tab3 (a3 int32, b3 int32) engine = MergeTree order by a3;

insert into tab1 values (1, 2);

insert into tab2 values (2, 3);
insert into tab2 values (6, 4);

insert into tab3 values (2, 3);
insert into tab3 values (5, 4);
insert into tab3 values (100, 4);

select 'join on OR chain (any left)';
select a2, b2 from tab2 any left join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2;
select '==';
select a3 == 5 OR a3 == 100, b3 from tab2 any left join tab3 on a2 = a3 or b2 = b3 ORDER BY a3, b3;
select '==';
select a2, b2, a3 == 5 OR a3 == 100, b3 from tab2 any left join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2, b3;
select '==';
select a1 from tab1 any left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1;
select '==';
select a1, b2 from tab1 any left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b2;
select '==';
select a1, b1, a2, b2 from tab1 any left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b1, a2, b2;
select '==';
select a2, b2 + 1 from tab1 any left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a2, b2 + 1;


select 'join on OR chain (any right)';
select a2, b2 from tab2 any right join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2;
select '==';
select a3, b3 from tab2 any right join tab3 on a2 = a3 or b2 = b3 ORDER BY a3, b3;
select '==';
select a2, b2, a3, b3 from tab2 any right join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2, a3, b3;
select '==';
select a1 from tab1 any right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1;
select '==';
select a1, b2 from tab1 any right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b2;
select '==';
select a1, b1, a2, b2 from tab1 any right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b1, a2, b2;
select '==';
select a2, b2 + 1 from tab1 any right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a2, b2 + 1;

SET any_join_distinct_right_table_keys = 1;
select 'any_join_distinct_right_table_keys = 1';
select a2, b2 from tab2 any right join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2;
select '==';
select a3, b3 from tab2 any right join tab3 on a2 = a3 or b2 = b3 ORDER BY a3, b3;


drop stream tab1;
drop stream tab2;
drop stream tab3;
