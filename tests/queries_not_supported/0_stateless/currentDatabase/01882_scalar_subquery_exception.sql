SET query_mode = 'table';
drop stream if exists nums_in_mem;
drop stream if exists nums_in_mem_dist;

create stream nums_in_mem(v uint64) engine=Memory;
insert into nums_in_mem select * from system.numbers limit 1000000;

create stream nums_in_mem_dist as nums_in_mem engine=Distributed('test_shard_localhost', currentDatabase(), nums_in_mem);

set prefer_localhost_replica = 0;
set max_rows_to_read = 100;

select
  count()
    /
  (select count() from nums_in_mem_dist where rand() > 0)
from system.one; -- { serverError 158 }

drop stream nums_in_mem;
drop stream nums_in_mem_dist;
