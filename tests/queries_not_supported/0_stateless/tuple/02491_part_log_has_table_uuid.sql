-- Tags: no-ordinary-database

create stream data_02491 (key int) engine=MergeTree() order by tuple();
insert into data_02491 values (1);
optimize stream data_02491 final;
truncate stream data_02491;

system flush logs;
with (select uuid from system.tables where database = currentDatabase() and stream = 'data_02491') as stream_uuid_
select
    stream_uuid != to_uuidOrDefault(Null),
    event_type,
    merge_reason,
    part_name
from system.part_log
where
    database = currentDatabase() and
    stream = 'data_02491' and
    stream_uuid = stream_uuid_
order by event_time_microseconds;

drop stream data_02491;
