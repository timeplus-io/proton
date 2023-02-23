drop stream if exists simple_agg_group_arrayLastArray;

-- { echo }
-- BAD_ARGUMENTS
select group_arrayLast(number+1) from numbers(5); -- { serverError BAD_ARGUMENTS }
select group_arrayLastArray([number+1]) from numbers(5); -- { serverError BAD_ARGUMENTS }
-- group_arrayLast by number
select group_arrayLast(1)(number+1) from numbers(5);
select group_arrayLast(3)(number+1) from numbers(5);
select group_arrayLast(3)(number+1) from numbers(10);
-- group_arrayLast by string
select group_arrayLast(3)((number+1)::string) from numbers(5);
select group_arrayLast(3)((number+1)::string) from numbers(10);
-- group_arrayLastArray
select group_arrayLastArray(3)([1,2,3,4,5,6]);
select group_arrayLastArray(3)(['1','2','3','4','5','6']);
-- group_arrayLastMerge
-- [10,8,9] + [10,8,9]     => [10,10,9] => [10,10,8] => [9,10,8]
--     ^          ^                  ^      ^^
-- (position to insert at)
select group_arrayLast(3)(number+1) state from remote('127.{1,1}', view(select * from numbers(10)));
select group_arrayLast(3)((number+1)::string) state from remote('127.{1,1}', view(select * from numbers(10)));
select group_arrayLast(3)([number+1]) state from remote('127.{1,1}', view(select * from numbers(10)));
select group_arrayLast(100)(number+1) state from remote('127.{1,1}', view(select * from numbers(10)));
select group_arrayLast(100)((number+1)::string) state from remote('127.{1,1}', view(select * from numbers(10)));
select group_arrayLast(100)([number+1]) state from remote('127.{1,1}', view(select * from numbers(10)));
-- SimpleAggregateFunction
create stream simple_agg_group_arrayLastArray (key int, value SimpleAggregateFunction(group_arrayLastArray(5), array(uint64))) engine=AggregatingMergeTree() order by key;
insert into simple_agg_group_arrayLastArray values (1, [1,2,3]), (1, [4,5,6]), (2, [4,5,6]), (2, [1,2,3]);
select * from simple_agg_group_arrayLastArray order by key, value;
system stop merges simple_agg_group_arrayLastArray;
insert into simple_agg_group_arrayLastArray values (1, [7,8]), (2, [7,8]);
select * from simple_agg_group_arrayLastArray order by key, value;
select * from simple_agg_group_arrayLastArray final order by key, value;
