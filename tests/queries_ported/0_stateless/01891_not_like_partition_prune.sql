drop stream if exists test;

create stream test (a string)  Engine MergeTree order by a partition by a;
insert into test values('1'), ('1.1'), ('1.2'), ('1.12');

select * from test where a like '1%1' order by a;
select * from test where a not like '1%1' order by a;
select * from test where a not like '1%2' order by a;
drop stream test;
