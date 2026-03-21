drop stream if exists 10661_stream;

create stream 10661_stream(id int, status int) primary key id settings mode='versioned_kv';

create materialized view 10661_mv as
select id
from 10661_stream
where status = 0
group by id
emit stream periodic 100ms repeat;

insert into 10661_stream(id, status) values (1, 0)(2, 0);
select sleep(3) format Null;

select distinct id from 10661_mv order by id settings query_mode='table';

drop view if exists 10661_mv;
drop stream if exists 10661_stream;
