drop stream if exists test;

create stream test (project low_cardinality(string)) engine=MergeTree() order by project;
insert into test values ('val1'), ('val2'), ('val3');

select sum(project in ('val1', 'val2')) from test;
set force_primary_key = 1;
select sum(project in ('val1', 'val2')) from test where project in ('val1', 'val2');
select count() from test where project in ('val1', 'val2');
select project in ('val1', 'val2') from test where project in ('val1', 'val2');

drop stream test;
