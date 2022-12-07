-- Tags: race

-- create temporary table dest00153 (`s` aggregate_function(groupUniqArray, string)) engine Memory;
create temporary stream dest00153 (`s` aggregate_function(group_array, string)) engine Memory;
-- insert into dest00153 select groupUniqArrayState(RefererDomain) FROM test.hits group by URLDomain;
insert into dest00153 select group_array_state(RefererDomain) FROM test.hits group by URLDomain;
