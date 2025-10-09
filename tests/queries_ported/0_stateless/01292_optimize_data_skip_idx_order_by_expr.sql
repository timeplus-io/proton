drop stream if exists data_01292;

create stream data_01292 (
    key int,
    index key_idx (key) type minmax granularity 1
) Engine=MergeTree() ORDER BY (key+0);

insert into data_01292 values (1);

optimize stream data_01292 final;

select * from data_01292 where key > 0;

drop stream if exists data_01292;
