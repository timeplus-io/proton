DROP VIEW IF EXISTS v;

create view v (s low_cardinality(string), n uint8) as select 'test' as s, to_uint8(number) as n from numbers(10000000);

-- this is what allows mem usage to go really high
set max_block_size=10000000000;

set max_memory_usage = '1Gi';

select s, sum(n) from v group by s format Null;

DROP VIEW v;
