SET query_mode = 'table';
drop stream if exists orin_test;

create stream orin_test (c1 int32) engine=Memory;
insert into orin_test values(1), (100);

select minus(c1 = 1 or c1=2 or c1 =3, c1=5) from orin_test;

drop stream orin_test;
