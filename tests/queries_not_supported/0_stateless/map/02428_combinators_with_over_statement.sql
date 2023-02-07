drop stream if exists test;
create stream test (x aggregate_function(uniq, uint64), y int64) engine=Memory;
insert into test select uniqState(number) as x, number as y from numbers(10) group by number;
select uniqStateMap(map(1, x)) OVER (PARTITION BY y) from test;
select uniqStateForEach([x]) OVER (PARTITION BY y) from test;
select uniqStateResample(30, 75, 30)([x], 30) OVER (PARTITION BY y) from test;
select uniqStateForEachMapForEach([map(1, [x])]) OVER (PARTITION BY y) from test;
select uniqStateDistinctMap(map(1, x)) OVER (PARTITION BY y) from test;
drop stream test;

