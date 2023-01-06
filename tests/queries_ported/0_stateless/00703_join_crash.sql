SET query_mode = 'table';
drop stream IF EXISTS tab1;
drop stream IF EXISTS tab1_copy;

create stream tab1 (a1 int32, b1 int32) engine = MergeTree order by a1;
create stream tab1_copy (a1 int32, b1 int32) engine = MergeTree order by a1;

insert into tab1(a1,b1) values (1, 2);
insert into tab1_copy(a1,b1) values (2, 3);

select tab1.a1, tab1_copy.a1, tab1.b1 from tab1 any left join tab1_copy on tab1.b1 + 3 = tab1_copy.b1 + 2;

drop stream tab1;
drop stream tab1_copy;
