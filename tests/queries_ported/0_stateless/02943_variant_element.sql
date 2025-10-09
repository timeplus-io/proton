set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;

select variant_element(NULL::variant(string, uint64), 'uint64') from numbers(4);
select variant_element(number::variant(string, uint64), 'uint64') from numbers(4);
select variant_element(number::variant(string, uint64), 'string') from numbers(4);
select variant_element((number % 2 ? NULL : number)::variant(string, uint64), 'uint64') from numbers(4);
select variant_element((number % 2 ? NULL : number)::variant(string, uint64), 'string') from numbers(4);
-- select variant_element((number % 2 ? NULL : 'str_' || to_string(number))::low_cardinality(nullable(string))::variant(low_cardinality(string), uint64), 'low_cardinality(string)') from numbers(4);
-- select variant_element(NULL::low_cardinality(nullable(string))::variant(low_cardinality(string), uint64), 'low_cardinality(string)') from numbers(4);
select variant_element((number % 2 ? NULL : number)::variant(array(uint64), uint64), 'array(uint64)') from numbers(4);
select variant_element(NULL::variant(array(uint64), uint64), 'array(uint64)') from numbers(4);
select variant_element(number % 2 ? NULL : range(number + 1), 'array(uint64)') from numbers(4);

select variant_element([[(number % 2 ? NULL : number)::variant(string, uint64)]], 'uint64') from numbers(4);
