set allow_suspicious_low_cardinality_types = 1;
SET query_mode = 'table';
SELECT CAST(NULL, 'LowCardinality(Nullable(int8))');

drop stream if exists lc_null_int8_defnull;
create stream lc_null_int8_defnull (val LowCardinality(Nullable(int8)) DEFAULT NULL) ENGINE = MergeTree order by tuple();
insert into lc_null_int8_defnull values (1);
select * from lc_null_int8_defnull values;
alter stream lc_null_int8_defnull add column val2 LowCardinality(Nullable(int8)) DEFAULT NULL;
insert into lc_null_int8_defnull values (2, 3);
select * from lc_null_int8_defnull order by val;
drop stream if exists lc_null_int8_defnull;

