-- Tags: global

SET query_mode = 'table';
drop stream if exists xp;
drop stream if exists xp_d;

create stream xp(i uint64, j uint64) engine MergeTree order by i settings index_granularity = 1;
create stream xp_d as xp engine Distributed(test_shard_localhost, currentDatabase(), xp);

insert into xp select number, number + 2 from numbers(10);

set max_rows_to_read = 4; -- 2 from numbers, 2 from tables
select * from xp where i in (select * from numbers(2));
select * from xp where i global in (select * from numbers(2));
select * from xp_d where i in (select * from numbers(2));

set max_rows_to_read = 6; -- 2 from numbers, 2 from GLOBAL temp table (pushed from numbers), 2 from local xp
select * from xp_d where i global in (select * from numbers(2));

drop stream if exists xp;
drop stream if exists xp_d;
