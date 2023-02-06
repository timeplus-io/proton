-- Tags: long

drop stream if exists buffer_02231;
drop stream if exists out_02231;
drop stream if exists in_02231;
drop stream if exists mv_02231;

-- To reproduce leak of memory tracking of aggregate states,
-- background flush is required.
create stream buffer_02231
(
    key int,
    v1 aggregate_function(group_array, string)
) engine=Buffer(current_database(), 'out_02231',
    /* layers= */1,
    /* min/max time  */ 86400, 86400,
    /* min/max rows  */ 1e9, 1e9,
    /* min/max bytes */ 1e12, 1e12,
    /* flush time */    1
);
create stream out_02231 as buffer_02231 engine=Null();
create stream in_02231 (number int) engine=Null();

-- Create lots of INSERT blocks with MV
create materialized view mv_02231 to buffer_02231 as select
    number as key,
    group_arrayState(to_string(number)) as v1
from in_02231
group by key;

insert into in_02231 select * from numbers(10e6) settings max_memory_usage='300Mi';

drop stream buffer_02231;
drop stream out_02231;
drop stream in_02231;
drop stream mv_02231;
