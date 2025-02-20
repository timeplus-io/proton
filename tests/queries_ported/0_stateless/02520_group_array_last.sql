set query_mode='table';
drop stream if exists simple_agg_group_array_last_array;

-- { echo }
-- NUMBER_OF_ARGUMENTS_DOESNT_MATCH
select group_array_last(number+1) from numbers(5); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select group_array_last_array([number+1]) from numbers(5); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- group_array_last by number
select group_array_last(number+1, 1) from numbers(5);
select group_array_last(number+1, 3) from numbers(5);
select group_array_last(number+1, 3) from numbers(10);
-- group_array_last by String
select group_array_last((number+1)::string, 3) from numbers(5);
select group_array_last((number+1)::string, 3) from numbers(10);
-- group_array_last_array
select group_array_last_array([1,2,3,4,5,6], 3);
select group_array_last_array(['1','2','3','4','5','6'], 3);
-- group_array_last_merge
-- [10,8,9] + [10,8,9]     => [10,10,9] => [10,10,8] => [9,10,8]
--     ^          ^                  ^      ^^
-- (position to insert at)
select group_array_last(number+1, 3) from (select * from numbers(10));
select group_array_last((number+1)::string, 3) from (select * from numbers(10));
select group_array_last([number+1], 3) from (select * from numbers(10));
select group_array_last(number+1, 100) from (select * from numbers(10));
select group_array_last((number+1)::string, 100) from (select * from numbers(10));
select group_array_last([number+1], 100) from (select * from numbers(10));
-- SimpleAggregateFunction
create stream simple_agg_group_array_last_array (key int, value simple_aggregate_function(group_array_last_array(5), array(uint64))) engine=MergeTree() order by key;
insert into simple_agg_group_array_last_array (key, value) values (1, [1,2,3]), (1, [4,5,6]), (2, [4,5,6]), (2, [1,2,3]);
select sleep(3);
select key, group_array_last_array(value, 5) from simple_agg_group_array_last_array group by key order by key;
