drop stream if exists x;

create stream x (i int, j int) engine MergeTree partition by i order by j settings index_granularity = 1;

insert into x values (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6);

set max_rows_to_read = 3;

select * from x where _partition_id = partition_id(1);

set max_rows_to_read = 5; -- one row for subquery + subquery

select * from x where _partition_id in (select partition_id(number + 1) from numbers(1));

-- trivial count optimization test
set max_rows_to_read = 2; -- one row for subquery + subquery itself
-- TODO: Relax the limits because we might build prepared set twice with _minmax_count_projection
set max_rows_to_read = 3;
select count() from x where _partition_id in (select partition_id(number + 1) from numbers(1));

drop stream x;

drop stream if exists mt;

create stream mt (n uint64) engine=MergeTree order by n partition by n % 10;

set max_rows_to_read = 200;

insert into mt select * from numbers(100);

select * from mt where to_uint64(substr(_part, 1, position(_part, '_') - 1)) = 1;

drop stream mt;
