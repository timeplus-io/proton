select deltaSum(array_join([1, 2, 3]));
select deltaSum(array_join([1, 2, 3, 0, 3, 4]));
select deltaSum(array_join([1, 2, 3, 0, 3, 4, 2, 3]));
select deltaSum(array_join([1, 2, 3, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select deltaSum(array_join([1, 2, 3, 0, 0, 0, 0, 3, 3, 3, 3, 3, 4, 2, 3]));
select deltaSumMerge(rows) from
(
    select * from
    (
        select deltaSumState(array_join([0, 1])) as rows
        union all
        select deltaSumState(array_join([4, 5])) as rows
    ) order by rows
);
select deltaSumMerge(rows) from
(
    select * from
    (
        select deltaSumState(array_join([4, 5])) as rows
        union all
        select deltaSumState(array_join([0, 1])) as rows
    ) order by rows
);
select deltaSum(array_join([2.25, 3, 4.5]));
select deltaSumMerge(rows) from
(
    select * from
    (
        select deltaSumState(array_join([0.1, 0.3, 0.5])) as rows
        union all
        select deltaSumState(array_join([4.1, 5.1, 6.6])) as rows
    ) order by rows
);
select deltaSumMerge(rows) from
(
    select * from
    (
        select deltaSumState(array_join([3, 5])) as rows
        union all
        select deltaSumState(array_join([1, 2])) as rows
        union all
        select deltaSumState(array_join([4, 6])) as rows
    ) order by rows
);
