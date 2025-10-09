drop stream if exists test;
create stream test (x uint64) engine=Memory();
set insert_null_as_default=1;
insert into test select number % 2 ? NULL : 42 as x from numbers(2);
select * from test order by x;
drop stream test;

create stream test (x low_cardinality(string) default 'Hello') engine=Memory();
insert into test select (number % 2 ? NULL : 'World')::low_cardinality(nullable(string)) from numbers(2);
select * from test order by x;
drop stream test;

