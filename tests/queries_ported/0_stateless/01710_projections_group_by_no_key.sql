drop stream if exists projection_without_key;

create stream projection_without_key (key uint32, PROJECTION x (SELECT sum(key) group by key % 3)) engine MergeTree order by key;
insert into projection_without_key select number from numbers(1000);
select sum(key) from projection_without_key settings allow_experimental_projection_optimization = 1;
select sum(key) from projection_without_key settings allow_experimental_projection_optimization = 0;

drop stream projection_without_key;
