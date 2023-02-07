set allow_suspicious_low_cardinality_types=1;
drop stream if exists test;
create stream test (x low_cardinality(int32)) engine=Memory;
insert into test select 1;
insert into test select 2;
select x + 1e10 from test order by 1e10, x;
select x + (1e10 + 1e20) from test order by (1e10 + 1e20), x;
select x + (pow(2, 2) + pow(3, 2)) from test order by (pow(2,2) + pow(3, 2)), x;
drop stream test;
