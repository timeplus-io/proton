drop stream if exists t_distinct_limit;

create stream t_distinct_limit (d Date, id int64)
engine = MergeTree partition by to_YYYYMM(d) order by d;

set max_threads = 10;

insert into t_distinct_limit select '2021-12-15', -1 from numbers(1e6);
insert into t_distinct_limit select '2021-12-15', -1 from numbers(1e6);
insert into t_distinct_limit select '2021-12-15', -1 from numbers(1e6);
insert into t_distinct_limit select '2022-12-15', 1 from numbers(1e6);
insert into t_distinct_limit select '2022-12-15', 1 from numbers(1e6);
insert into t_distinct_limit select '2022-12-16', 11 from numbers(1);
insert into t_distinct_limit select '2023-12-16', 12 from numbers(1);
insert into t_distinct_limit select '2023-12-16', 13 from numbers(1);
insert into t_distinct_limit select '2023-12-16', 14 from numbers(1);

set max_block_size = 1024;

select id from
(
    select distinct id from remote('127.0.0.1,127.0.0.2', current_database(),t_distinct_limit) limit 10
)
order by id;

drop stream if exists t_distinct_limit;
