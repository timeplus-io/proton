set join_algorithm = 'partial_merge';

select * from (select dummy as val from system.one) s1 any left join (select dummy as val from system.one) s2 using val;
select * from (select to_low_cardinality(dummy) as val from system.one) s1 any left join (select dummy as val from system.one) s2 using val;
select * from (select dummy as val from system.one) s1 any left join (select to_low_cardinality(dummy) as val from system.one) s2 using val;
select * from (select to_low_cardinality(dummy) as val from system.one) s1 any left join (select to_low_cardinality(dummy) as val from system.one) s2 using val;
select * from (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s1 any left join (select dummy as val from system.one) s2 using val;
select * from (select dummy as val from system.one) s1 any left join (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s2 using val;
select * from (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s1 any left join (select to_low_cardinality(dummy) as val from system.one) s2 using val;
select * from (select to_low_cardinality(dummy) as val from system.one) s1 any left join (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s2 using val;
select * from (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s1 any left join (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s2 using val;
select '-';
select * from (select dummy as val from system.one) s1 any left join (select dummy as val from system.one) s2 on val + 0 = val * 1; -- { serverError 352 }
select * from (select dummy as val from system.one) s1 any left join (select dummy as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select to_low_cardinality(dummy) as val from system.one) s1 any left join (select dummy as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select dummy as val from system.one) s1 any left join (select to_low_cardinality(dummy) as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select to_low_cardinality(dummy) as val from system.one) s1 any left join (select to_low_cardinality(dummy) as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s1 any left join (select dummy as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select dummy as val from system.one) s1 any left join (select to_low_cardinality(to_nullable(dummy)) as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s1 any left join (select to_low_cardinality(dummy) as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select to_low_cardinality(dummy) as val from system.one) s1 any left join (select to_low_cardinality(to_nullable(dummy)) as rval from system.one) s2 on val + 0 = rval * 1;
select * from (select to_low_cardinality(to_nullable(dummy)) as val from system.one) s1 any left join (select to_low_cardinality(to_nullable(dummy)) as rval from system.one) s2 on val + 0 = rval * 1;
select '-';
select * from (select number as l from system.numbers limit 3) s1 any left join (select number as r from system.numbers limit 3) s2 on l + 1 = r * 1;
select * from (select to_low_cardinality(number) as l from system.numbers limit 3) s1 any left join (select number as r from system.numbers limit 3) s2 on l + 1 = r * 1;
select * from (select number as l from system.numbers limit 3) s1 any left join (select to_low_cardinality(number) as r from system.numbers limit 3) s2 on l + 1 = r * 1;
select * from (select to_low_cardinality(number) as l from system.numbers limit 3) s1 any left join (select to_low_cardinality(number) as r from system.numbers limit 3) s2 on l + 1 = r * 1;
select * from (select to_low_cardinality(to_nullable(number)) as l from system.numbers limit 3) s1 any left join (select to_low_cardinality(number) as r from system.numbers limit 3) s2 on l + 1 = r * 1;
select * from (select to_low_cardinality(number) as l from system.numbers limit 3) s1 any left join (select to_low_cardinality(to_nullable(number)) as r from system.numbers limit 3) s2 on l + 1 = r * 1;
select * from (select to_low_cardinality(to_nullable(number)) as l from system.numbers limit 3) s1 any left join (select to_low_cardinality(to_nullable(number)) as r from system.numbers limit 3) s2 on l + 1 = r * 1;
