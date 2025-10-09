set allow_experimental_variant_type=1;

select NULL::variant(string, uint64);
select 42::uint64::variant(string, uint64);
select 42::uint32::variant(string, uint64); -- {serverError CANNOT_CONVERT_TYPE}
select now()::variant(string, uint64); -- {serverError CANNOT_CONVERT_TYPE}
select CAST(number % 2 ? NULL : number, 'variant(string, uint64)') from numbers(4);
select 'Hello'::low_cardinality(string)::variant(low_cardinality(string), uint64);
select 'Hello'::low_cardinality(nullable(string))::variant(low_cardinality(string), uint64);
select 'NULL'::low_cardinality(nullable(string))::variant(low_cardinality(string), uint64);
select 'Hello'::low_cardinality(nullable(string))::variant(low_cardinality(string), uint64);
select CAST(CAST(number % 2 ? NULL : 'Hello', 'low_cardinality(nullable(string))'), 'variant(low_cardinality(string), uint64)') from numbers(4);

select NULL::variant(string, uint64)::uint64;
select NULL::variant(string, uint64)::nullable(uint64);
select '42'::variant(string, uint64)::uint64;
select 'str'::variant(string, uint64)::uint64; -- {serverError CANNOT_PARSE_TEXT}
select CAST(multi_if(number % 3 == 0, NULL::variant(string, uint64), number % 3 == 1, 'Hello'::variant(string, uint64), number::variant(string, uint64)), 'nullable(string)') from numbers(6);
select CAST(multi_if(number == 1, NULL::variant(string, uint64), number == 2, 'Hello'::variant(string, uint64), number::variant(string, uint64)), 'uint64') from numbers(6); -- {serverError CANNOT_PARSE_TEXT}


select number::variant(uint64)::variant(string, uint64)::variant(array(string), string, uint64) from numbers(2);
select 'str'::variant(string, uint64)::variant(string, array(uint64)); -- {serverError CANNOT_CONVERT_TYPE}
