SET query_mode = 'table';
drop stream if exists cardinality;
create stream cardinality (x string) engine = MergeTree order by tuple();
insert into cardinality (x) select concat('v', to_string(number)) from numbers(10);
alter stream cardinality add column y LowCardinality(string);
select * from cardinality;
drop stream if exists cardinality;
