select to_type_name(if(to_low_cardinality(number % 2), 1, 2)) from numbers(1);
select to_type_name(multi_if(to_low_cardinality(number % 2), 1, 1, 2, 3)) from numbers(1);
select to_type_name(to_low_cardinality(number % 2) and 2) from numbers(1);
select to_type_name(to_low_cardinality(number % 2) or 2) from numbers(1);

