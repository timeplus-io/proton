SELECT count() AS cnt WHERE 0 HAVING cnt = 0;

select cnt from (select count() as cnt where 0) where cnt = 0;

select cnt from (select count() as cnt from system.one where 0) where cnt = 0;

select sum from (select sum(dummy) as sum from system.one where 0) where sum = 0;

set aggregate_functions_null_for_empty=1;
select sum from (select sum(dummy) as sum from system.one where 0) where sum is null;
