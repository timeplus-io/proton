select CAST(toLowCardinality(val) as uint64) from (select array_join(['1']) as val);
select to_uint64(toLowCardinality(val)) from (select array_join(['1']) as val);
select 1 % toLowCardinality(val) from (select array_join([1]) as val);
select gcd(1, toLowCardinality(val)) from (select array_join([1]) as val);
