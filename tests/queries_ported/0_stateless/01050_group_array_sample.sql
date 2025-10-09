select k, group_array_sample(v, 10, 123456) from (select number % 4 as k, number as v from numbers(1024)) group by k order by k;

-- different seed
select k, group_array_sample(v, 10, 1) from (select number % 4 as k, number as v from numbers(1024)) group by k order by k;
