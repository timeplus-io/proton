-- https://github.com/ClickHouse/ClickHouse/issues/9810
select cast(1 as string)
from (select 1 as iid) as t1
join (select '1' as sid) as t2 on t2.sid = cast(t1.iid as string);

-- even simpler cases
select cast(7 as string), * from (select 3 "'string'");
select cast(7 as string), * from (select number "'string'" FROM numbers(2));
SELECT concat('xyz', 'abc'), * FROM (SELECT 2 AS "'xyz'");
with 3 as "1" select 1, "1"; -- { serverError 352 }

-- https://github.com/ClickHouse/ClickHouse/issues/9953
select 1, * from (select 2 x) a left join (select 1, 3 y) b on y = x;
select 1, * from (select 2 x, 1) a right join (select 3 y) b on y = x;
select null, isConstant(null), * from (select 2 x) a left join (select null, 3 y) b on y = x;
select null, isConstant(null), * from (select 2 x, null) a right join (select 3 y) b on y = x;

-- other cases with joins and constants

select cast(1, 'uint8') from (select array_join([1, 2]) as a) t1 left join (select 1 as b) t2 on b = ignore('uint8'); -- { serverError 403 }

select isConstant('uint8'), to_fixed_string('hello', to_uint8(substring('uint8', 5, 1))) from (select array_join([1, 2]) as a) t1 left join (select 1 as b) t2 on b = ignore('uint8');  -- { serverError 403 }

-- https://github.com/ClickHouse/ClickHouse/issues/20624
select 2 as `to_string(x)`, x from (select 1 as x);
