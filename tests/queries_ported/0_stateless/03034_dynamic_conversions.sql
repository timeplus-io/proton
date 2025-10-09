set allow_experimental_dynamic_type=1;
set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;

select number::dynamic as d, dynamic_type(d) from numbers(3);
select number::dynamic(max_types=0) as d, dynamic_type(d) from numbers(3);
select number::dynamic::uint64 as v from numbers(3);
select number::dynamic::string as v from numbers(3);
select number::dynamic::Date as v from numbers(3);
select number::dynamic::array(uint64) as v from numbers(3); -- {serverError TYPE_MISMATCH}
select number::dynamic::variant(uint64, string) as v, variant_type(v) from numbers(3);
select (number % 2 ? NULL : number)::dynamic as d, dynamic_type(d) from numbers(3);

select multi_if(number % 4 == 0, number, number % 4 == 1, 'str_' || to_string(number), number % 4 == 2, range(number), NULL)::dynamic as d, dynamic_type(d) from numbers(6);
select multi_if(number % 4 == 0, number, number % 4 == 1, 'str_' || to_string(number), number % 4 == 2, range(number), NULL)::dynamic(max_types=0) as d, dynamic_type(d) from numbers(6);
select multi_if(number % 4 == 0, number, number % 4 == 1, 'str_' || to_string(number), number % 4 == 2, range(number), NULL)::dynamic(max_types=1) as d, dynamic_type(d) from numbers(6);
select multi_if(number % 4 == 0, number, number % 4 == 1, 'str_' || to_string(number), number % 4 == 2, range(number), NULL)::dynamic(max_types=2) as d, dynamic_type(d) from numbers(6);

select number::dynamic(max_types=2)::dynamic(max_types=3) as d from numbers(3);
select number::dynamic(max_types=2)::dynamic(max_types=1) as d from numbers(3);
select multi_if(number % 4 == 0, number, number % 4 == 1, 'str_' || to_string(number), number % 4 == 2, range(number), NULL)::dynamic(max_types=2)::dynamic(max_types=1) as d, dynamic_type(d) from numbers(6);
select multi_if(number % 4 == 0, number, number % 4 == 1, to_date(number), number % 4 == 2, range(number), NULL)::dynamic(max_types=4)::dynamic(max_types=3) as d, dynamic_type(d) from numbers(6);

drop stream if exists test;
create stream test (d dynamic) engine = Memory;
insert into test values (NULL), (42), ('42.42'), (true), ('e10');
select d::float64 from test;
select d::nullable(float64) from test;
select d::string from test;
select d::nullable(string) from test;
select d::uint64 from test; -- {serverError CANNOT_PARSE_TEXT}
select d::nullable(uint64) from test;
select d::Date from test; -- {serverError CANNOT_PARSE_DATE}

