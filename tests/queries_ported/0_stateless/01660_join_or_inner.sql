SET joined_subquery_requires_alias = 0;

drop stream if exists tab2;
drop stream if exists tab3;

create stream tab2 (a2 int32, b2 int32) engine = MergeTree order by a2;
create stream tab3 (a3 int32, b3 int32) engine = MergeTree order by a3;

insert into tab2 values (2, 3);
insert into tab2 values (6, 4);
insert into tab2 values (998, 999);

insert into tab3 values (2, 3);
insert into tab3 values (5, 4);
insert into tab3 values (100, 4);
insert into tab3 values (1998, 1999);

set max_threads = 1;

SET any_join_distinct_right_table_keys = 0;
select 'any_join_distinct_right_table_keys = 0';
select tab2.*, tab3.* from tab2 any join tab3 on a2 = a3 or b2 = b3;
select '==';
select tab2.*, tab3.* from tab2 any join tab3 on b2 = b3 or a2 = a3;

SET any_join_distinct_right_table_keys = 1;
select 'any_join_distinct_right_table_keys = 1';
select tab2.*, tab3.* from tab2 any join tab3 on a2 = a3 or b2 = b3;
select '==';
select tab2.*, tab3.* from tab2 any join tab3 on b2 = b3 or a2 = a3;

SELECT 1 FROM (select 1 as a, 1 as aa, 1 as aaa, 1 as aaaa) A JOIN (select 1 as b, 1 as bb, 1 as bbb, 1 as bbbb, 1 as bbbbb) B ON a = b OR a = bb OR a = bbb OR a = bbbb OR aa = b OR aa = bb OR aa = bbb OR aa = bbbb OR aaa = b OR aaa = bb OR aaa = bbb OR aaa = bbbb OR aaaa = b OR aaaa = bb OR aaaa = bbb OR aaaa = bbbb OR a = bbbbb OR aa = bbbbb;

drop stream tab2;
drop stream tab3;
