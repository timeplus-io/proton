SET query_mode = 'table';
set allow_suspicious_low_cardinality_types = 1;
drop stream if exists lc_00800_2;
create stream lc_00800_2 (val LowCardinality(uint64)) engine = MergeTree order by val;
insert into lc_00800_2 select number % 123 from system.numbers limit 100000;
select distinct(val) from lc_00800_2 order by val;
drop stream if exists lc_00800_2
;
