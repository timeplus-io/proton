set allow_experimental_analyzer=0; -- The result type for if function with constant is different with analyzer. It wil be fixed after refactoring around constants in analyzer.

set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;

select to_type_name(res), if(1, [1,2,3], 'str_1') as res;
select to_type_name(res), if(1, [1,2,3], 'str_1'::nullable(string)) as res;

select to_type_name(res), if(0, [1,2,3], 'str_1') as res;
select to_type_name(res), if(0, [1,2,3], 'str_1'::nullable(string)) as res;

select to_type_name(res), if(NULL, [1,2,3], 'str_1') as res;
select to_type_name(res), if(NULL, [1,2,3], 'str_1'::nullable(string)) as res;

select to_type_name(res), if(materialize(NULL::nullable(uint8)), [1,2,3], 'str_1') as res;
select to_type_name(res), if(materialize(NULL::nullable(uint8)), [1,2,3], 'str_1'::nullable(string)) as res;

select to_type_name(res), if(1, materialize([1,2,3]), 'str_1') as res;
select to_type_name(res), if(1, materialize([1,2,3]), 'str_1'::nullable(string)) as res;

select to_type_name(res), if(0, materialize([1,2,3]), 'str_1') as res;
select to_type_name(res), if(0, materialize([1,2,3]), 'str_1'::nullable(string)) as res;

select to_type_name(res), if(NULL, materialize([1,2,3]), 'str_1') as res;
select to_type_name(res), if(NULL, materialize([1,2,3]), 'str_1'::nullable(string)) as res;

select to_type_name(res), if(materialize(NULL::nullable(uint8)), materialize([1,2,3]), 'str_1') as res;
select to_type_name(res), if(materialize(NULL::nullable(uint8)), materialize([1,2,3]), 'str_1'::nullable(string)) as res;

select to_type_name(res), if(1, [1,2,3], materialize('str_1')) as res;
select to_type_name(res), if(1, [1,2,3], materialize('str_1')::nullable(string)) as res;

select to_type_name(res), if(0, [1,2,3], materialize('str_1')) as res;
select to_type_name(res), if(0, [1,2,3], materialize('str_1')::nullable(string)) as res;

select to_type_name(res), if(NULL, [1,2,3], materialize('str_1')) as res;
select to_type_name(res), if(NULL, [1,2,3], materialize('str_1')::nullable(string)) as res;

select to_type_name(res), if(materialize(NULL::nullable(uint8)), [1,2,3], materialize('str_1')) as res;
select to_type_name(res), if(materialize(NULL::nullable(uint8)), [1,2,3], materialize('str_1')::nullable(string)) as res;


select to_type_name(res), if(0, range(number + 1), 'str_' || to_string(number)) as res from numbers(4);
select to_type_name(res), if(0, range(number + 1), ('str_' || to_string(number))::nullable(string)) as res from numbers(4);

select to_type_name(res), if(1, range(number + 1), 'str_' || to_string(number)) as res from numbers(4);
select to_type_name(res), if(1, range(number + 1), ('str_' || to_string(number))::nullable(string)) as res from numbers(4);

select to_type_name(res), if(NULL, range(number + 1), 'str_' || to_string(number)) as res from numbers(4);
select to_type_name(res), if(NULL, range(number + 1), ('str_' || to_string(number))::nullable(string)) as res from numbers(4);

select to_type_name(res), if(materialize(NULL::nullable(uint8)), range(number + 1), 'str_' || to_string(number)) as res from numbers(4);
select to_type_name(res), if(materialize(NULL::nullable(uint8)), range(number + 1), ('str_' || to_string(number))::nullable(string)) as res from numbers(4);

select to_type_name(res), if(number % 2, range(number + 1), 'str_' || to_string(number)) as res from numbers(4);
select to_type_name(res), if(number % 2, range(number + 1), ('str_' || to_string(number))::nullable(string)) as res from numbers(4);

select to_type_name(res), if(number % 2, range(number + 1), ('str_' || to_string(number))::low_cardinality(string)) as res from numbers(4);
select to_type_name(res), if(number % 2, range(number + 1), ('str_' || to_string(number))::low_cardinality(nullable(string))) as res from numbers(4);


select to_type_name(res), multi_if(number % 3 == 0, range(number + 1), number % 3 == 1, number, 'str_' || to_string(number)) as res from numbers(6);
select to_type_name(res), multi_if(number % 3 == 0, range(number + 1), number % 3 == 1, number,  ('str_' || to_string(number))::nullable(string)) as res from numbers(6);
select to_type_name(res), multi_if(number % 3 == 0, range(number + 1), number % 3 == 1, number, ('str_' || to_string(number))::low_cardinality(string)) as res from numbers(6);
select to_type_name(res), multi_if(number % 3 == 0, range(number + 1), number % 3 == 1, number, ('str_' || to_string(number))::low_cardinality(nullable(string))) as res from numbers(6);


select to_type_name(res), array_cast(1, 'str_1', 2, 'str_2') as res;
select to_type_name(res), array_cast([1, 2, 3], ['str_1', 'str_2', 'str_3']) as res;
select to_type_name(res), array_cast(array_cast([1, 2, 3], ['str_1', 'str_2', 'str_3']), [1, 2, 3]) as res;
select to_type_name(res), array_cast([1, 2, 3], [[1, 2, 3]]) as res;

select to_type_name(res), map_cast('a', 1, 'b', 'str_1') as res;
select to_type_name(res), map_cast('a', 1, 'b', map_cast('c', 2, 'd', 'str_1')) as res;
select to_type_name(res), map_cast('a', 1, 'b', [1, 2, 3], 'c', [[4, 5, 6]]) as res;

