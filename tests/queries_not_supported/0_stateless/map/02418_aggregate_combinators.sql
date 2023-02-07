select uniqStateMap(map(1, number)) from numbers(10);
select uniqStateForEachMapForEachMap(map(1, [map(1, [number, number])])) from numbers(10);
select uniqStateForEachResample(30, 75, 30)([number, number + 1], 30) from numbers(10);
select uniqStateMapForEachResample(30, 75, 30)([map(1, number)], 30) from numbers(10);
select uniqStateForEachMerge(x) as y from (select uniqStateForEachState([number]) as x from numbers(10));
select uniqMerge(y[1]) from (select uniqStateForEachMerge(x) as y from (select uniqStateForEachState([number]) as x from numbers(10)));

drop stream if exists test;
create stream test (x map(uint8, aggregate_function(uniq, uint64))) engine=Memory;
insert into test select uniqStateMap(map(1, number)) from numbers(10);
select * from test format Null;
select mapApply(k, v -> (k, finalizeAggregation(v)), x) from test;
truncate stream test;
drop stream test;

create stream test (x map(uint8, array(map(uint8, array(aggregate_function(uniq, uint64)))))) engine=Memory;
insert into test select uniqStateForEachMapForEachMap(map(1, [map(1, [number, number])])) from numbers(10);
select mapApply(k, v -> (k, array_map(x -> mapApply(k, v -> (k, array_map(x -> finalizeAggregation(x), v)), x), v)), x) from test;
select * from test format Null;
truncate stream test;
drop stream test;

create stream test (x array(array(aggregate_function(uniq, uint64)))) engine=Memory;
insert into test select uniqStateForEachResample(30, 75, 30)([number, number + 1], 30) from numbers(10);
select array_map(x -> array_map(x -> finalizeAggregation(x), x), x) from test;
select * from test format Null;
truncate stream test;
drop stream test;

create stream test (x array(array(map(uint8, aggregate_function(uniq, uint64))))) engine=Memory;
insert into test select uniqStateMapForEachResample(30, 75, 30)([map(1, number)], 30) from numbers(10);
select array_map(x -> array_map(x -> mapApply(k, v -> (k, finalizeAggregation(v)), x), x), x) from test;
select * from test format Null;
truncate stream test;
drop stream test;

