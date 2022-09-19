-- Tags: no-parallel
SET query_mode = 'table';
select 'NativeBlockInputStream';
select to_type_name(dict), dict, lowCardinalityIndices(dict), lowCardinalityKeys(dict) from (select '123_' || toLowCardinality(v) as dict from (select array_join(['a', 'bb', '', 'a', 'ccc', 'a', 'bb', '', 'dddd']) as v));
select '-';
select to_type_name(dict), dict, lowCardinalityIndices(dict), lowCardinalityKeys(dict) from (select '123_' || toLowCardinality(v) as dict from (select array_join(['a', Null, 'bb', '', 'a', Null, 'ccc', 'a', 'bb', '', 'dddd']) as v));

select 'MergeTree';

drop stream if exists lc_small_dict;
drop stream if exists lc_big_dict;

create stream lc_small_dict (str StringWithDictionary) engine = MergeTree order by str;
create stream lc_big_dict (str StringWithDictionary) engine = MergeTree order by str;

insert into lc_small_dict select to_string(number % 1000) from system.numbers limit 1000000;
insert into lc_big_dict select to_string(number) from system.numbers limit 1000000;

detach table lc_small_dict;
detach table lc_big_dict;

attach table lc_small_dict;
attach table lc_big_dict;

select sum(toUInt64OrZero(str)) from lc_small_dict;
select sum(toUInt64OrZero(str)) from lc_big_dict;

drop stream if exists lc_small_dict;
drop stream if exists lc_big_dict;

