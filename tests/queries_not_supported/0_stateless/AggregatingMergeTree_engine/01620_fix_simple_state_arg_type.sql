SET query_mode = 'table';
drop stream if exists ay;

create stream ay engine AggregatingMergeTree order by i as select 1 i, sumSimpleState(10) group by i;
insert into ay values(40, 60);
insert into ay values(40, 50);
insert into ay values(20, 30);
optimize table ay;
select * from ay;
insert into ay values(20, 30), (40, 10);
optimize table ay;
select * from ay;

drop stream if exists ay;
