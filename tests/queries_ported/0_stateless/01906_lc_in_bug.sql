--drop stream if exists tab;
--create stream tab (x low_cardinality(string)) engine = MergeTree order by tuple();

--insert into tab values ('a'), ('bb'), ('a'), ('cc');

--select count() as c, x in ('a', 'bb') as g from tab group by g order by c;

drop stream if exists test;

-- https://github.com/ClickHouse/ClickHouse/issues/44503
CREATE stream test(key int32) ENGINE = MergeTree ORDER BY (key);
insert into test select int_div(number,100) from numbers(10000000);
SELECT COUNT() FROM test WHERE key <= 100000 AND (NOT (to_low_cardinality('') IN (SELECT '')));