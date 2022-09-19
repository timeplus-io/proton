select runningDifference(x) from (select array_join([0, 1, 5, 10]) as x);
select '-';
select runningDifference(x) from (select array_join([2, Null, 3, Null, 10]) as x);
select '-';
select runningDifference(x) from (select array_join([Null, 1]) as x);
select '-';
select runningDifference(x) from (select array_join([Null, Null, 1, 3, Null, Null, 5]) as x);

