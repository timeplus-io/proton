SET query_mode = 'table';
drop stream if exists num_10m;
create stream num_10m (number uint64) engine = MergeTree order by tuple();
insert into num_10m select * from numbers(10000000);

select * from (select sum(number) from num_10m union all select sum(number) from num_10m) limit 1 settings max_block_size = 1024;

drop stream if exists num_10m;
