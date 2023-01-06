-- Tags: no-parallel
SET query_mode = 'table';
drop stream if exists lc_dict_reading;
create stream lc_dict_reading (val uint64, str StringWithDictionary, pat string) engine = MergeTree order by val;
insert into lc_dict_reading select number, if(number < 8192 * 4, number % 100, number) as s, s from system.numbers limit 1000000;
select sum(to_uint64(str)), sum(to_uint64(pat)) from lc_dict_reading where val < 8129 or val > 8192 * 4;
drop stream if exists lc_dict_reading;

