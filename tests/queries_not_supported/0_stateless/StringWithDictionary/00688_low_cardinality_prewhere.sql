SET query_mode = 'table';
drop stream if exists lc_prewhere;
create stream lc_prewhere (key uint64, val uint64, str StringWithDictionary, s string) engine = MergeTree order by key settings index_granularity = 8192;
insert into lc_prewhere select number, if(number < 10 or number > 8192 * 9, 1, 0), to_string(number) as s, s from system.numbers limit 100000;
select sum(to_uint64(str)), sum(to_uint64(s)) from lc_prewhere prewhere val == 1;
drop stream if exists lc_prewhere;
