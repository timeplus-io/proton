SET joined_subquery_requires_alias = 0;

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

select 'join on OR chain (all left)';
select a2, b2 from tab2 all left join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2;
select '==';
select a3, b3 from tab2 all left join tab3 on a2 = a3 or b2 = b3 ORDER BY a3, b3;
select '==';
select a2, b2, a3, b3 from tab2 all left join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2, a3, b3;
select '==';
select a1 from tab1 all left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1;
select '==';
select a1, b2 from tab1 all left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b2;
select '==';
select a1, b1, a2, b2 from tab1 all left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b1, a2, b2;
select '==';
select a2, b2 + 1 from tab1 all left join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a2, b2 + 1;

select 'join on OR chain (all right)';
select a2, b2 from tab2 all right join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2;
select '==';
select a3, b3 from tab2 all right join tab3 on a2 = a3 or b2 = b3 ORDER BY a3, b3;
select '==';
select a2, b2, a3, b3 from tab2 all right join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2, a3, b3;
select '==';
select a1 from tab1 all right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1;
select '==';
select a1, b2 from tab1 all right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b2;
select '==';
select a1, b1, a2, b2 from tab1 all right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b1, a2, b2;
select '==';
select a2, b2 + 1 from tab1 all right join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a2, b2 + 1;

select 'join on OR chain (full)';
select a2, b2 from tab2 full join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2;
select '==';
select a3, b3 from tab2 full join tab3 on a2 = a3 or b2 = b3 ORDER BY a3, b3;
select '==';
select a2, b2, a3, b3 from tab2 full join tab3 on a2 = a3 or b2 = b3 ORDER BY a2, b2, a3, b3;
select '==';
select a1 from tab1 full join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1;
select '==';
select a1, b2 from tab1 full join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b2;
select '==';
select a1, b1, a2, b2 from tab1 full join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a1, b1, a2, b2;
select '==';
select a2, b2 + 1 from tab1 full join tab2 on b1 + 1 = a2 + 1 or a1 + 4 = b2 + 2 ORDER BY a2, b2 + 1;

drop stream tab1;
drop stream tab2;
drop stream tab3;
