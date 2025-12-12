drop view if exists 99112_mv;
drop stream if exists 99112_v;
drop stream if exists 99112_randv;
drop stream if exists 99112_count_stable;

create stream 99112_v(id int);

create random stream 99112_randv(id int);

select sleep(2) FORMAT Null;

create materialized view 99112_mv into 99112_v as select * from 99112_randv;

select sleep(2) FORMAT Null;

system pause materialized view 99112_mv;

select sleep(2) FORMAT Null;
create stream 99112_count_stable(cnt int);

select sleep(2) FORMAT Null;
insert into 99112_count_stable(cnt) select count() as cnt from table(99112_v);

select sleep(2) FORMAT Null;

insert into 99112_count_stable(cnt) select count() as cnt from table(99112_v);

select sleep(2) FORMAT Null;

-- Check if both rows have identical cnt
select 
    min(cnt) = max(cnt) as is_same
from table(99112_count_stable);

drop view if exists 99112_mv;
drop stream if exists 99112_v;
drop stream if exists 99112_randv;
drop stream if exists 99112_count_stable;
