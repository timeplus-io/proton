drop stream if exists unhex_in_fix_string_table;
create stream unhex_in_fix_string_table ( dt Date, s1 fixed_string(20), s2 string) engine=MergeTree partition by dt order by tuple();
insert into unhex_in_fix_string_table values(today(), '436C69636B486F757365', '436C69636B486F757365');
select unhex(s1), unhex(s2) from unhex_in_fix_string_table;
