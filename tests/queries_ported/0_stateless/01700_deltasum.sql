select delta_sum(array_join([1, 2, 3]));
select delta_sum(array_join([1, 2, 3, 0, 3, 4]));
select delta_sum(array_join([1, 2, 3, 0, 3, 4, 2, 3]));
select delta_sum(array_join([1, 2, 3, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select delta_sum(array_join([1, 2, 3, 0, 0, 0, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select delta_sum_merge(rows) as delta_sum from
(
    select * from
    (
        select delta_sum_state(array_join([0, 1])) as rows
        union all
        select delta_sum_state(array_join([4, 5])) as rows
    ) order by rows
) order by delta_sum;
select delta_sum_merge(rows) as delta_sum from
(
    select * from
    (
        select delta_sum_state(array_join([4, 5])) as rows
        union all
        select delta_sum_state(array_join([0, 1])) as rows
    ) order by rows
) order by delta_sum;
select delta_sum(array_join([2.25, 3, 4.5]));
select delta_sum_merge(rows) as delta_sum from
(
    select * from
    (
        select delta_sum_state(array_join([0.1, 0.3, 0.5])) as rows
        union all
        select delta_sum_state(array_join([4.1, 5.1, 6.6])) as rows
    ) order by rows
) order by delta_sum;
select delta_sum_merge(rows) as delta_sum from
(
    select * from
    (
        select delta_sum_state(array_join([3, 5])) as rows
        union all
        select delta_sum_state(array_join([1, 2])) as rows
        union all
        select delta_sum_state(array_join([4, 6])) as rows
    ) order by rows
) order by delta_sum;
