drop stream if exists a;
drop stream if exists b;

create stream a (i int, j int) engine Log;
create materialized view b engine Log as select countState(*) from a;

insert into a values (1, 2);
select countMerge(*) from b;

drop stream b;
drop stream a;
